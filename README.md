# Postgres spreadsheet view

A quick experiment.

1. ~~Spin up postgres docker container~~
2. ~~UI:~~
   a. ~~Query editor in topbar -- pushes data into datagrid~~
   b. ~~Data grid -- 2d table of input elements, matching data size~~
   c. ~~Bottom (add new row) button~~
3. ~~Edit row wise using primary key as id, push back out to database~~ (transaction processing)
   a. ~~Need row-table provenance for each cell.~~
4. Either:
   - Forward errors to frontend in modal + reject action, or
   - Accept edit
5. Propagate changes to other clients
   - Can use pgoutput / wal2json for notification mechanism (via web sockets)

## TODO

- [ ] 4. Optimistic update to frontend + loading spinner if accept takes a while -- can do this because of 204 status -- easy heuristic
- [x] 4. Error modals
- [x] 5. WAL streaming to other clients.
   - [ ] pg_lineage extensions
      - [x] column / table provenance
      - [o] row level provenance

         - [x] rebuild queries with better edithandles (column + pk), and do filtration to present clean edithandles to users 
            - fixes missing edithandle problem
   - parse WAL stream and provide edithandles for tuples
      - fixes reactivity because we reconcile on frontend via edithandle
   - switch architecture to "subscribe" to a query, and only push the relevant 
      changes instead of the entire WAL to the client.
   - [x] Store queries

- Add a notimplemented error in case we're editing a join.
- [ ] switch from modifying group by clause to group keys, 
   where group keys are computed per group
   - ignore this case for now, group keys / aggregates are a particularly difficult case

1. schema introspection endpoint + ui
2. better navigation / baked (SELECT * FROM table;) / saved (SELECT [...complicated mess...]) queries
3. better collaboration (change notifications, live cursor)
4. time travel + undo (need activities table)
5. csv imports (no cap frfr)