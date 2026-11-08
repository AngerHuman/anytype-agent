import { concat, concatMap, filter, map, Observable, type OperatorFunction, of, scan } from "rxjs";

export function splitLines(): OperatorFunction<string, string> {
  return (source$: Observable<string>) =>
    concat(source$, of("\n")).pipe(
      scan(
        ({ buffer }, chunk) => {
          const combined = buffer + chunk;
          const lines = combined.split("\n");
          const rest = lines.pop() ?? "";
          return { buffer: rest, lines };
        },
        { buffer: "", lines: [] as string[] },
      ),
      concatMap(({ lines }) => lines),
      map((line) => line.trim()),
      filter(Boolean),
    );
}
