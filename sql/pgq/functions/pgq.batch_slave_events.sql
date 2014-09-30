
-- Group: Low-level event handling

create or replace function pgq.get_batch_slave_events(
    in x_batch_id   bigint,
    in x_queue text,
    in x_consumer text,
    in cur_tick_id bigint,
    in prev_tick_id bigint,
    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_slave_events(1)
--
--      Get all events in batch from local read slave.
--      Because we are reading from local read slave, we
--      need more information instead a single batch_id.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_queue         - name of the event queue
--      x_consumer      - name of the consumer
--      cur_tick_id     - current tick id
--      prev_tick_id    - last tick id
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare
    sql text;
    queue_id        integer;
    sub_id          integer;
    cons_id         integer;
    errmsg text;
begin
    select s.sub_queue, s.sub_consumer, s.sub_id
        into queue_id, cons_id, sub_id
        from pgq.consumer c,
             pgq.queue q,
             pgq.subscription s
        where q.queue_name = x_queue
          and c.co_name = x_consumer
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(x_queue, 'NULL')
            || '/'
            || coalesce(x_consumer, 'NULL');
        raise exception '%', errmsg;
    end if;

    sql := pgq.batch_slave_event_sql(x_batch_id, sub_id, cur_tick_id, prev_tick_id);
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute sql
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql; -- no perms needed


create or replace function pgq.batch_slave_event_sql(
    x_batch_id bigint,
    x_sub_id bigint,
    cur_tick_id bigint,
    prev_tick_id bigint)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_slave_event_sql(1)
--      Creates SELECT statement that fetches events for this batch from local readslave.
--
-- Parameters:
--      x_batch_id    - ID of a active batch.
--      x_sub_id      - ID of the subscription of this batch
--      cur_tick_id   - current tick id
--      prev_tick_id  - last tick id
--
-- Returns:
--      SQL statement.
-- ----------------------------------------------------------------------

-- ----------------------------------------------------------------------
-- Algorithm description:
--      Given 2 snapshots, sn1 and sn2 with sn1 having xmin1, xmax1
--      and sn2 having xmin2, xmax2 create expression that filters
--      right txid's from event table.
--
--      Simplest solution would be
--      > WHERE ev_txid >= xmin1 AND ev_txid <= xmax2
--      >   AND NOT txid_visible_in_snapshot(ev_txid, sn1)
--      >   AND txid_visible_in_snapshot(ev_txid, sn2)
--
--      The simple solution has a problem with long transactions (xmin1 very low).
--      All the batches that happen when the long tx is active will need
--      to scan all events in that range.  Here is 2 optimizations used:
--
--      1)  Use [xmax1..xmax2] for range scan.  That limits the range to
--      txids that actually happened between two snapshots.  For txids
--      in the range [xmin1..xmax1] look which ones were actually
--      committed between snapshots and search for them using exact
--      values using IN (..) list.
--
--      2) As most TX are short, there could be lot of them that were
--      just below xmax1, but were committed before xmax2.  So look
--      if there are ID's near xmax1 and lower the range to include
--      them, thus decresing size of IN (..) list.
-- ----------------------------------------------------------------------
declare
    rec             record;
    sql             text;
    tbl             text;
    arr             text;
    part            text;
    select_fields   text;
    retry_expr      text;
    batch           record;
begin
    select s.sub_last_tick, s.sub_next_tick, s.sub_id, s.sub_queue,
           txid_snapshot_xmax(last.tick_snapshot) as tx_start,
           txid_snapshot_xmax(cur.tick_snapshot) as tx_end,
           last.tick_snapshot as last_snapshot,
           cur.tick_snapshot as cur_snapshot
        into batch
        from pgq.subscription s, pgq.tick last, pgq.tick cur
        where s.sub_id = x_sub_id
          and last.tick_queue = s.sub_queue
          and last.tick_id = prev_tick_id
          and cur.tick_queue = s.sub_queue
          and cur.tick_id = cur_tick_id;
    if not found then
        raise exception 'batch not found';
    end if;

    batch.sub_last_tick := prev_tick_id;
    batch.sub_next_tick := cur_tick_id;

    -- load older transactions
    arr := '';
    for rec in
        -- active tx-es in prev_snapshot that were committed in cur_snapshot
        select id1 from
            txid_snapshot_xip(batch.last_snapshot) id1 left join
            txid_snapshot_xip(batch.cur_snapshot) id2 on (id1 = id2)
        where id2 is null
        order by 1 desc
    loop
        -- try to avoid big IN expression, so try to include nearby
        -- tx'es into range
        if batch.tx_start - 100 <= rec.id1 then
            batch.tx_start := rec.id1;
        else
            if arr = '' then
                arr := rec.id1::text;
            else
                arr := arr || ',' || rec.id1::text;
            end if;
        end if;
    end loop;

    -- must match pgq.event_template
    select_fields := 'select ev_id, ev_time, ev_txid, ev_retry, ev_type,'
        || ' ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4';
    retry_expr :=  ' and (ev_owner is null or ev_owner = '
        || batch.sub_id::text || ')';

    -- now generate query that goes over all potential tables
    sql := '';
    for rec in
        select xtbl from pgq.batch_slave_event_tables(x_batch_id, x_sub_id, cur_tick_id, prev_tick_id) xtbl
    loop
        tbl := pgq.quote_fqname(rec.xtbl);
        -- this gets newer queries that definitely are not in prev_snapshot
        part := select_fields
            || ' from pgq.tick cur, pgq.tick last, ' || tbl || ' ev '
            || ' where cur.tick_id = ' || batch.sub_next_tick::text
            || ' and cur.tick_queue = ' || batch.sub_queue::text
            || ' and last.tick_id = ' || batch.sub_last_tick::text
            || ' and last.tick_queue = ' || batch.sub_queue::text
            || ' and ev.ev_txid >= ' || batch.tx_start::text
            || ' and ev.ev_txid <= ' || batch.tx_end::text
            || ' and txid_visible_in_snapshot(ev.ev_txid, cur.tick_snapshot)'
            || ' and not txid_visible_in_snapshot(ev.ev_txid, last.tick_snapshot)'
            || retry_expr;
        -- now include older tx-es, that were ongoing
        -- at the time of prev_snapshot
        if arr <> '' then
            part := part || ' union all '
                || select_fields || ' from ' || tbl || ' ev '
                || ' where ev.ev_txid in (' || arr || ')'
                || retry_expr;
        end if;
        if sql = '' then
            sql := part;
        else
            sql := sql || ' union all ' || part;
        end if;
    end loop;
    if sql = '' then
        raise exception 'could not construct sql for batch %', x_batch_id;
    end if;
    return sql || ' order by 1';
end;
$$ language plpgsql;  -- no perms needed



create or replace function pgq.batch_slave_event_tables(
    x_batch_id bigint,
    x_sub_id bigint,
    cur_tick_id bigint,
    prev_tick_id bigint)
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_slave_event_tables(1)
--
--     Returns set of table names where this batch events may reside from local readslave.
--
-- Parameters:
--     x_batch_id    - ID of a active batch.
--     x_sub_id      - ID of the subscription of this batch
--     cur_tick_id   - current tick id
--     prev_tick_id  - last tick id
-- ----------------------------------------------------------------------
declare
    nr                    integer;
    tbl                   text;
    use_prev              integer;
    use_next              integer;
    batch                 record;
begin
    select
           txid_snapshot_xmin(last.tick_snapshot) as tx_min, -- absolute minimum
           txid_snapshot_xmax(cur.tick_snapshot) as tx_max, -- absolute maximum
           q.queue_data_pfx, q.queue_ntables,
           q.queue_cur_table, q.queue_switch_step1, q.queue_switch_step2
        into batch
        from pgq.tick last, pgq.tick cur, pgq.subscription s, pgq.queue q
        where cur.tick_id = cur_tick_id
          and cur.tick_queue = s.sub_queue
          and last.tick_id = prev_tick_id
          and last.tick_queue = s.sub_queue
          and s.sub_id = x_sub_id
          and q.queue_id = s.sub_queue;
    if not found then
        raise exception 'Cannot find data for batch %', x_batch_id;
    end if;

    -- if its definitely not in one or other, look into both
    if batch.tx_max < batch.queue_switch_step1 then
        use_prev := 1;
        use_next := 0;
    elsif batch.queue_switch_step2 is not null
      and (batch.tx_min > batch.queue_switch_step2)
    then
        use_prev := 0;
        use_next := 1;
    else
        use_prev := 1;
        use_next := 1;
    end if;

    if use_prev then
        nr := batch.queue_cur_table - 1;
        if nr < 0 then
            nr := batch.queue_ntables - 1;
        end if;
        tbl := batch.queue_data_pfx || '_' || nr::text;
        return next tbl;
    end if;

    if use_next then
        tbl := batch.queue_data_pfx || '_' || batch.queue_cur_table::text;
        return next tbl;
    end if;

    return;
end;
$$ language plpgsql; -- no perms needed
