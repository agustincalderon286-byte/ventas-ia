// Keep database connections until in-flight HTTP work and the current worker finish.
export function installGracefulShutdown({ server, signals = process, setDraining, stopWorker, workerBusy,
  disconnect, exit = code => process.exit(code), delay = ms => new Promise(resolve => setTimeout(resolve, ms)),
  timeoutMs = 280000, schedule = setTimeout, cancel = clearTimeout }) {
  let shutdown;
  const drain = () => {
    if (shutdown) return shutdown;
    setDraining();
    stopWorker();
    shutdown = (async () => {
      const deadline = schedule(() => { server.closeAllConnections?.(); exit(1); }, timeoutMs);
      deadline.unref?.();
      try {
        await Promise.all([
          new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())),
          (async () => { while (workerBusy()) await delay(100); })()
        ]);
        await disconnect();
        cancel(deadline);
        exit(0);
      } catch {
        cancel(deadline);
        exit(1);
      }
    })();
    return shutdown;
  };
  signals.once("SIGTERM", drain);
  signals.once("SIGINT", drain);
  return drain;
}
