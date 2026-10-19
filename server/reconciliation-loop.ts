/** One in-flight reconciliation per lane. Slow model/evidence work in another
 * lane cannot delay this timer; shutdown waits for the actual operation. */
export class ReconciliationLoop {
  private stopped = false;
  private completion?: Promise<void>;
  private wake?: () => void;
  private timer?: ReturnType<typeof setTimeout>;
  private pendingWake = false;

  constructor(
    private readonly reconcile: () => Promise<unknown>,
    private readonly onError: (error: unknown) => Promise<unknown>,
    private readonly intervalMs = 5000,
  ) {}

  start() {
    if (this.stopped) throw Error('RECONCILIATION_STOPPED');
    this.completion ??= this.run();
  }

  /** Coalesce a burst into one extra pass; never overlap the active pass. */
  requestWake() {
    if (this.stopped) return;
    this.pendingWake = true;
    if (this.timer) clearTimeout(this.timer);
    this.wake?.();
  }

  requestStop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.wake?.();
  }

  async close() {
    this.requestStop();
    await this.completion;
  }

  private async run() {
    while (!this.stopped) {
      this.pendingWake = false;
      try {
        await this.reconcile();
      } catch (error) {
        // A failed diagnostic write must not permanently disable the lane.
        try { await this.onError(error); } catch { /* retry remains scheduled */ }
      }
      if (this.stopped) break;
      if (this.pendingWake) continue;
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        this.timer = setTimeout(resolve, this.intervalMs);
      });
      this.timer = undefined;
      this.wake = undefined;
    }
  }
}
