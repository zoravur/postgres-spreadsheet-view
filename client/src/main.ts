import {
  init,
  classModule,
  propsModule,
  styleModule,
  eventListenersModule,
  h,
} from "snabbdom";
import { datagrid } from "./datagrid";
import { Store } from "./store";
// import { triggerModal } from "./triggerModal";
import { fetchApi } from "./util/fetchApi";
import Swal from "sweetalert2";
import { connectWS, subscribeWS } from "./socket";

export const patch = init([
  // Init patch function with chosen modules
  classModule, // makes it easy to toggle classes
  propsModule, // for setting properties on DOM elements
  styleModule, // handles styling on elements with support for animations
  eventListenersModule, // attaches event listeners
]);

type State = {
  query: string;
  results: any;
  editing: any;
  loading: number;
  pendingEdits: number;
};

const initialState: State = {
  query: "SELECT * FROM actor ORDER BY actor_id LIMIT 5;",
  results: null,
  editing: null,
  loading: 0,
  pendingEdits: 0,
};

const store = new Store(initialState);

store.on("EVENT/#query/change", (state, action) => {
  return { ...state, query: action.payload };
});

store.on(
  "HTTP/api/query",
  (state, _action) => {
    return { ...state, loading: state.loading + 1 };
  },
  async (_action, state) => {
    try {
      const data = await fetchApi("/api/query", {
        method: "POST",
        body: state.query,
        useJson: false,
      });
      subscribeWS(state.query);

      store.dispatch({ type: "DATA/results", payload: data });
    } catch (err: any) {
      store.dispatch({ type: "HTTP/api/query/FAILURE", payload: err.message });
    }
  }
);

store.on(
  "HTTP/api/edit",
  (state, _action) => {
    return { ...state, pendingEdits: state.pendingEdits + 1 };
  },
  async (action, _state) => {
    try {
      const data = await fetchApi("/api/edit", {
        method: "POST",
        body: action.payload,
      });
      store.dispatch({ type: "HTTP/api/edit/SUCCESS", payload: data });
    } catch (err: any) {
      store.dispatch({ type: "HTTP/api/edit/FAILURE", payload: err.message });
    }
  }
);

store.on(/HTTP\/api\/edit\/(SUCCESS|FAILURE)/, (state, _action) => {
  return { ...state, pendingEdits: state.pendingEdits - 1 };
});

store.on(/FAILURE/, null, (action, _state) => {
  Swal.fire("Failure", `<pre>${action.payload}</pre>`, "error");
  // triggerModal(action.payload);
});

store.on("HTTP/api/query/FAILURE", (state, _action) => {
  return { ...state, loading: state.loading - 1 };
});

// assume: const handleIndex = new Map<string, { row: number; col: string }[]>();

store.on("UI/edit", (state, action) => {
  return { ...state, editing: action.payload };
});

// identity: (editHandle, resultCol)
const key = (handle: string, col: string) => `${handle}::${col}`;

type Cell = { editHandle?: string; value: any };
type Row = Record<string, Cell>;
type Ref = { row: number; col: string };

const handleIndex = new Map<string, Ref[]>(); // key = key(handle,col)

/** Build/replace results */
store.on("DATA/results", (state, action) => {
  const results = action.payload as Row[];

  handleIndex.clear();
  for (let i = 0; i < results.length; i++) {
    const row = results[i];
    for (const col of Object.keys(row)) {
      const h = row[col]?.editHandle;
      if (!h) continue;

      const k = key(h, col);
      let refs = handleIndex.get(k);
      if (!refs) handleIndex.set(k, (refs = []));
      refs.push({ row: i, col });
    }
  }

  return { ...state, results, loading: state.loading - 1 };
});

/** Apply socket updates without cross-column bleed */
store.on("SOCKET/UPDATE", (state, action) => {
  const old = state.results;
  if (!old) return state;

  const results = old.slice();
  const touched = new Set<number>();

  for (const upd of action.payload as Array<Record<string, Cell>>) {
    for (const [col, cell] of Object.entries(upd)) {
      const h = cell?.editHandle;
      if (!h) continue;

      const refs = handleIndex.get(key(h, col));
      if (!refs) continue;

      for (const { row, col: refCol } of refs) {
        if (!touched.has(row)) {
          results[row] = { ...results[row] }; // clone row once
          touched.add(row);
        }
        results[row][refCol] = cell; // replace only that column’s cells
      }
    }
  }

  return { ...state, results };
});

store.subscribe((state: typeof initialState, _action: any, _prev: any) => {
  console.log(state);
  vnode = patch(vnode, view(state));
});

const view = (state: any) =>
  h("div", {}, [
    h(
      "textarea#query",
      {
        props: { rows: 5, cols: 60 },
        on: {
          change: (ev: Event) => {
            store.dispatch({
              type: "EVENT/#query/change",
              payload: (ev.target as HTMLTextAreaElement).value,
            });
          },
        },
      },
      state.query
    ),
    h(
      "button#run",
      {
        on: {
          click: async () => {
            store.dispatch({ type: "HTTP/api/query", payload: null });
          },
        },
      },
      "Run"
    ),
    h(
      "pre#result",
      datagrid({
        data: state.results,
        loading: state.loading !== 0,
        onEdit: async (i, key, val) => {
          store.dispatch({
            type: "HTTP/api/edit",
            payload: {
              editHandle: state.results[i][key].editHandle,
              column: key,
              value: val,
            },
          });
        },
        editing: state.editing,
        setEditing: (cell: { row: number; col: keyof any } | null) => {
          store.dispatch({ type: "UI/edit", payload: cell });
        },
      })
    ),
  ]);

const container = document.getElementById("app")!;
let vnode = patch(container, view(store.state));

connectWS(`ws://localhost:8080/api/ws`, (update: any) => {
  store.dispatch({ type: "SOCKET/UPDATE", payload: update });
});

// function rerender() {
//   vnode = patch(vnode, view(store.state));
// }
