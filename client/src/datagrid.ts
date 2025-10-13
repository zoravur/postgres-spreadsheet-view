import { h, type VNode } from "snabbdom";

interface EditableGridProps<T extends Record<string, unknown>> {
  data: T[];
  onEdit?: (rowIndex: number, key: keyof T, value: string) => void;
  editing: { row: number; col: keyof any } | null;
  setEditing: (cell: { row: number; col: keyof any } | null) => void;
}

export function datagrid<T extends Record<string, unknown>>({
  data,
  onEdit,
  editing,
  setEditing,
}: EditableGridProps<T>): VNode {
  if (data.length === 0) return h("table");

  const columns = Object.keys(data[0]) as (keyof T)[];
  const numericCols = new Set(
    columns.filter((col) => typeof data[0][col] === "number")
  );
  const makeCell = (rowIdx: number, col: keyof T, value: unknown): VNode => {
    const isEditing = editing && editing.row === rowIdx && editing.col === col;

    if (isEditing) {
      return h("td", [
        h("input", {
          props: { value: String(value) },
          style: {
            width: "100%",
            textAlign: numericCols.has(col) ? "right" : "left",
          },
          on: {
            blur: (e: Event) => {
              const target = e.target as HTMLInputElement;
              setEditing(null);
              onEdit?.(rowIdx, col, target.value);
            },
            keydown: (e: KeyboardEvent) => {
              if (e.key === "Enter") {
                (e.target as HTMLInputElement).blur();
              }
            },
          },
        }),
      ]);
    }

    return h(
      "td",
      {
        style: {
          textAlign: numericCols.has(col) ? "right" : "left",
          padding: "4px 8px",
          cursor: "pointer",
        },
        on: {
          click: () => setEditing({ row: rowIdx, col }),
        },
      },
      String(value ?? "")
    );
  };

  const th = (col: keyof T): VNode =>
    h(
      "th",
      {
        style: {
          textAlign: numericCols.has(col) ? "right" : "left",
          padding: "4px 8px",
          borderBottom: "1px solid #ccc",
        },
      },
      String(col)
    );

  return h("table", { style: { borderCollapse: "collapse", width: "100%" } }, [
    h("thead", [h("tr", columns.map(th))]),
    h(
      "tbody",
      data.map((row, i) =>
        h(
          "tr",
          columns.map((col) => makeCell(i, col, row[col]))
        )
      )
    ),
  ]);
}
