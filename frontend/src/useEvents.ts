import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

interface ScanEvent {
  type: string;
  run_id?: number;
}

/** Subscribe to the server's SSE stream and invalidate affected queries live. */
export function useLiveEvents() {
  const qc = useQueryClient();
  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onmessage = (e) => {
      const event = JSON.parse(e.data) as ScanEvent;
      qc.invalidateQueries({ queryKey: ["stats"] });
      if (event.type.startsWith("run.")) {
        qc.invalidateQueries({ queryKey: ["runs"] });
        if (event.run_id != null) {
          qc.invalidateQueries({ queryKey: ["run", event.run_id] });
          qc.invalidateQueries({ queryKey: ["run-files", event.run_id] });
        }
      }
      if (event.type === "task.done") {
        qc.invalidateQueries({ queryKey: ["runs"] });
        qc.invalidateQueries({ queryKey: ["detections"] });
        qc.invalidateQueries({ queryKey: ["run-files"] });
      }
    };
    return () => es.close();
  }, [qc]);
}
