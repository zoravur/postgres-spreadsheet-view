# Postgres spreadsheet view

A quick experiment.

1. Spin up postgres docker container
2. UI:
   a. Query editor in topbar -- pushes data into datagrid
   b. Data grid -- 2d table of input elements, matching data size
   c. Bottom (add new row) button
3. Edit row wise using primary key as id, push back out to database (transaction processing)
   a. Need row-table provenance for each cell.
4. Either:
   - Forward errors to frontend in modal + reject action, or
   - Accept edit
5. Propagate changes to other clients
   - Can use pgoutput / wal2json for notification mechanism (via web sockets)
