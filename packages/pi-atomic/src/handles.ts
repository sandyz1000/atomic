import { randomUUID } from "node:crypto";

const handles = new Map<string, any[]>();

export function storeRows(rows: any[]): string {
  const handle = randomUUID();
  handles.set(handle, rows);
  return handle;
}

export function getRows(handle: string): any[] | undefined {
  return handles.get(handle);
}
