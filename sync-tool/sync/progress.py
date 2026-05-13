"""
Progress Display

Provides a unified progress bar interface that uses tqdm when available
and falls back to a simple text-based display when it isn't.

Usage:
    from sync.progress import progress_bar

    for item in progress_bar(items, desc="Processing", unit="items"):
        process(item)

    # Or with manual control:
    from sync.progress import ProgressTracker

    tracker = ProgressTracker(total=100, desc="Downloading")
    for i in range(100):
        do_work()
        tracker.update(1)
    tracker.close()
"""

from typing import Iterable, Optional

# Conditional import: use tqdm if installed, otherwise fall back
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


def progress_bar(
    iterable: Iterable,
    desc: str = "",
    total: Optional[int] = None,
    unit: str = "it",
    leave: bool = True,
    disable: bool = False,
) -> Iterable:
    """
    Wrap an iterable with a progress bar.

    If tqdm is installed, produces a rich progress bar with estimated
    time remaining and throughput. Otherwise, prints periodic text updates.

    Args:
        iterable: The iterable to wrap.
        desc: Description label shown before the bar (e.g., "Retrieving").
        total: Total number of items. If None, tries len(iterable).
        unit: Unit label (e.g., "accessions", "rows").
        leave: If True, keep the bar visible after completion.
        disable: If True, disable the progress display entirely.

    Returns:
        An iterable that displays progress as it's consumed.
    """
    if disable:
        return iterable

    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None

    if TQDM_AVAILABLE:
        return tqdm(
            iterable,
            desc=f"  {desc}" if desc else None,
            total=total,
            unit=f" {unit}" if unit else None,
            leave=leave,
            bar_format="  {desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            ncols=80,
        )
    else:
        return _FallbackProgress(iterable, desc=desc, total=total, unit=unit)


class ProgressTracker:
    """
    Manual progress tracker for operations where you can't wrap an iterable.

    Provides update() and close() methods for step-by-step tracking.
    Uses tqdm internally if available, otherwise prints text updates.

    Usage:
        tracker = ProgressTracker(total=500, desc="Writing rows")
        for row in rows:
            write(row)
            tracker.update(1)
        tracker.close()
    """

    def __init__(
        self,
        total: int,
        desc: str = "",
        unit: str = "it",
        disable: bool = False,
    ):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.disable = disable
        self._count = 0

        if disable:
            self._bar = None
        elif TQDM_AVAILABLE:
            self._bar = tqdm(
                total=total,
                desc=f"  {desc}" if desc else None,
                unit=f" {unit}" if unit else None,
                bar_format="  {desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ncols=80,
            )
        else:
            self._bar = None
            # Print initial message
            if desc:
                print(f"  {desc}: 0/{total} {unit}", end="", flush=True)

    def update(self, n: int = 1) -> None:
        """Advance the progress by n steps."""
        if self.disable:
            return

        self._count += n

        if TQDM_AVAILABLE and self._bar:
            self._bar.update(n)
        elif not TQDM_AVAILABLE:
            # Print updates at meaningful intervals to avoid flooding the terminal.
            # For small totals, update every step. For large totals, update at
            # every 5% or every 50 items, whichever is smaller.
            if self.total <= 20:
                interval = 1
            else:
                interval = min(max(self.total // 20, 1), 50)

            if self._count % interval == 0 or self._count >= self.total:
                pct = (self._count / self.total * 100) if self.total > 0 else 0
                print(
                    f"\r  {self.desc}: {self._count}/{self.total} {self.unit} ({pct:.0f}%)",
                    end="",
                    flush=True,
                )

    def close(self) -> None:
        """Finish the progress display."""
        if self.disable:
            return

        if TQDM_AVAILABLE and self._bar:
            self._bar.close()
        elif not TQDM_AVAILABLE:
            # Print final state and move to a new line
            if self.total > 0:
                print(
                    f"\r  {self.desc}: {self._count}/{self.total} {self.unit} (100%)",
                    flush=True,
                )
            else:
                print(flush=True)


class _FallbackProgress:
    """
    Simple text-based progress display used when tqdm is not installed.

    Wraps an iterable and prints periodic updates using carriage return
    to overwrite the current line. Updates at roughly 5% intervals to
    avoid excessive terminal output.
    """

    def __init__(
        self,
        iterable: Iterable,
        desc: str = "",
        total: Optional[int] = None,
        unit: str = "it",
    ):
        self._iterable = iterable
        self._desc = desc
        self._total = total
        self._unit = unit
        self._count = 0

        # Calculate update interval: every 5% for large iterables,
        # every item for small ones
        if self._total and self._total > 20:
            self._interval = max(self._total // 20, 1)
        else:
            self._interval = 1

    def __iter__(self):
        if self._desc and self._total:
            print(f"  {self._desc}: 0/{self._total} {self._unit}", end="", flush=True)
        elif self._desc:
            print(f"  {self._desc}: starting...", end="", flush=True)

        for item in self._iterable:
            yield item
            self._count += 1

            if self._count % self._interval == 0 or self._count == self._total:
                if self._total:
                    pct = self._count / self._total * 100
                    print(
                        f"\r  {self._desc}: {self._count}/{self._total} {self._unit} ({pct:.0f}%)",
                        end="",
                        flush=True,
                    )
                else:
                    print(
                        f"\r  {self._desc}: {self._count} {self._unit}",
                        end="",
                        flush=True,
                    )

        # Final newline after completion
        if self._total:
            print(
                f"\r  {self._desc}: {self._count}/{self._total} {self._unit} (100%)",
                flush=True,
            )
        else:
            print(f"\r  {self._desc}: {self._count} {self._unit} (done)", flush=True)

    def __len__(self):
        if self._total is not None:
            return self._total
        try:
            return len(self._iterable)
        except TypeError:
            raise TypeError("object has no len()")
