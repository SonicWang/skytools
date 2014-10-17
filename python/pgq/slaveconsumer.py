
"""Slave Consumer that runs in a Postgres readslave.
   It allocates a new batch from master and reads events
   locally by taking advantage of Postgres streaming replication.
   It also sends back a batch of events for retry (instead of sending
   back events one at a time).

"""

from pgq.consumer import Consumer

__all__ = ['SlaveConsumer']


# Event status codes
EV_UNTAGGED = -1
EV_RETRY = 0
EV_DONE = 1


class SlaveConsumer(Consumer):
    """Reads locally and writes to remote master.
    Retries events in batch.
    """

    def __init__(self, service_name, db_name, slave_db, args):
        """db_name is the remote master, slave_db is the local readslave
        """
        Consumer.__init__(self, service_name, db_name, args)
        self.slave_db = slave_db

    def work(self):
        """Do the work loop, once (internal).
        Returns: true if wants to be called again,
        false if script can sleep.

        This class allocates a new batch in master,
        then it checks locally if streaming replication
        has already caught up with regard to the batch_id + tick_id.
        It yes, then proceed to read locally all the events and process them.
        Otherwise return 0 and will retry the same batch later.
        """
        self.pgq_lazy_fetch = 0

        db = self.get_database(self.db_name)
        master_curs = db.cursor()

        self.stat_start()

        # acquire batch
        batch_id = self._load_next_batch(master_curs)
        db.commit()
        if batch_id == None:
            return 0
        slave_curs = self.get_database(self.slave_db).cursor()

        slave_curs.execute("""select tick_id from pgq.tick where tick_id = %s""",
            [self.batch_info['tick_id']])
        if slave_curs.rowcount <= 0:
            self.get_database(self.slave_db).commit()
            return 0

        # load events locally
        ev_list = self._load_batch_events(slave_curs, batch_id)
        self.get_database(self.slave_db).commit()

        # process events
        self._launch_process_batch(db, batch_id, ev_list)

        # done, ack to master
        self._finish_batch(master_curs, batch_id, ev_list)
        db.commit()
        self.stat_end(len(ev_list))

        return 1

    def _flush_retry(self, master_curs, batch_id, list):
        """Tag retry events."""

        retry = 0
        retry_list = []
        retry_time = None
        if self.pgq_lazy_fetch:
            for ev_id, stat in list.iter_status():
                if stat[0] == EV_RETRY:
                    retry_list.append(ev_id)
                    if not retry_time or stat[1] > retry_time:
                        retry_time = stat[1]
                    retry += 1
                elif stat[0] != EV_DONE:
                    raise Exception("Untagged event: id=%d" % ev_id)
        else:
            for ev in list:
                if ev._status == EV_RETRY:
                    retry_list.append(ev.id)
                    if not retry_time or ev.retry_time > retry_time:
                        retry_time = ev.retry_time
                    retry += 1
                elif ev._status != EV_DONE:
                    raise Exception("Untagged event: (id=%d, type=%s, data=%s, ex1=%s" % (
                                    ev.id, ev.type, ev.data, ev.extra1))

        # report weird events
        if retry:
            self._batch_retry(master_curs, batch_id, retry_list, retry_time)
            self.stat_increase('retry-events', retry)

    def _batch_retry(self, cx, batch_id, retry_list, retry_time):
        """send a batch of event ids for retry"""
        cx.execute("select pgq.batch_slave_event_retry(%s, %s, %s)",
                   [batch_id, '{' + ",".join(str(x) for x in retry_list) + '}', retry_time])

    def _load_batch_events(self, curs, batch_id):
        """Fetch all events for this batch."""

        # load events
        sql = "select * from pgq.get_batch_slave_events(%d, '%s', '%s', %d, %d)" % (
            batch_id, self.queue_name, self.consumer_name,
            self.batch_info['cur_tick_id'],
            self.batch_info['prev_tick_id'])
        if self.consumer_filter is not None:
            sql += " where %s" % self.consumer_filter
        curs.execute(sql)
        rows = curs.dictfetchall()

        # map them to python objects
        ev_list = []
        for r in rows:
            ev = self._make_event(self.queue_name, r)
            ev_list.append(ev)

        return ev_list
