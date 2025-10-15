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
import { connectWS } from "./socket";

export const patch = init([
  // Init patch function with chosen modules
  classModule, // makes it easy to toggle classes
  propsModule, // for setting properties on DOM elements
  styleModule, // handles styling on elements with support for animations
  eventListenersModule, // attaches event listeners
]);

const initialState = {
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

store.on("DATA/results", (state, action) => {
  return { ...state, results: action.payload, loading: state.loading - 1 };
});

store.on("UI/edit", (state, action) => {
  return { ...state, editing: action.payload };
});

store.on("SOCKET/api/data", null, (action, _state) => {
  console.log("SOCKET MESSAGE: ", action.payload);
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

connectWS(
  `ws://localhost:8080/api/ws`,
  function (this: WebSocket, ev: MessageEvent<any>): any {
    store.dispatch({ type: "SOCKET/api/data", payload: JSON.parse(ev.data) });
  }
);

// function rerender() {
//   vnode = patch(vnode, view(store.state));
// }
