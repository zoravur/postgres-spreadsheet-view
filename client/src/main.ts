import {
  init,
  classModule,
  propsModule,
  styleModule,
  eventListenersModule,
  h,
} from "snabbdom";
import { datagrid } from "./datagrid";

export const patch = init([
  // Init patch function with chosen modules
  classModule, // makes it easy to toggle classes
  propsModule, // for setting properties on DOM elements
  styleModule, // handles styling on elements with support for animations
  eventListenersModule, // attaches event listeners
]);

const state: {
  query: string;
  results: Object | null;
  editing: { row: number; col: keyof any } | null;
} = {
  query: "SELECT * FROM actor LIMIT 5;",
  results: null,
  editing: null,
};

const view = (state: any) =>
  h("div", {}, [
    h(
      "textarea#query",
      {
        props: { rows: 5, cols: 60 },
        on: {
          change: (ev: Event) => {
            console.log("event");
            state.query = (ev.target as HTMLTextAreaElement).value;
            console.log(state);
            rerender();
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
            const res = await fetch("/api/query", {
              method: "POST",
              body: state.query,
            });

            state.results = await res.json();
            console.log("results set");
            rerender();
          },
        },
      },
      "Run"
    ),
    h(
      "pre#result",
      state.results
        ? datagrid({
            data: state.results,
            onEdit: (i, key, val) => {
              state.results[i][key] = val; // mutate or trigger signal
              rerender();
            },
            editing: state.editing,
            setEditing: (cell: { row: number; col: keyof any } | null) => {
              state.editing = cell;
              rerender();
            },
          })
        : "(empty)"
    ),
  ]);

const container = document.getElementById("app")!;
let vnode = patch(container, view(state));

function rerender() {
  console.log(state);
  vnode = patch(vnode, view(state));
}
