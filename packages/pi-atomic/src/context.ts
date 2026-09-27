import { Context, SqlContext } from "@atomic-compute/js";

// Deployment mode, local IP, and worker hosts all come from the environment
// (`ATOMIC_DEPLOYMENT_MODE`, `ATOMIC_LOCAL_IP`, `~/hosts.conf`), so distributed
// execution needs no options here.
export const ctx = new Context();
export const sqlCtx = new SqlContext();
