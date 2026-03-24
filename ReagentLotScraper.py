#!/usr/bin/env python3
"""
Multi-Entry Field Extractor GUI (Excel + CSV) — Column A/B only

Select ONE input file (.xlsx/.xlsm/.xltx/.xltm/.csv). Scan for repeated "entries"
(blocks) that contain a complete set of these labels in Column A:
  - Sample ID
  - Reagent Lot ID
  - Start Time

For each label found, extract the corresponding value from Column B on the same row.
Columns beyond B are ignored entirely.

An "entry" is emitted whenever all three fields have been captured.
Scanning continues until end-of-sheet/workbook/file.

Outputs a new Excel file with one row per entry:
  Column A: Sample ID
  Column B: Reagent Lot ID
  Column C: Start Time

Requirements:
  pip install pandas openpyxl
"""

from __future__ import annotations

import os
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk


TARGET_FIELDS = ["Sample ID", "Reagent Lot ID", "Start Time"]
EXCEL_EXTENSIONS = (".xlsx", ".xlsm", ".xltx", ".xltm")
CSV_EXTENSIONS = (".csv",)
SUPPORTED_EXTENSIONS = EXCEL_EXTENSIONS + CSV_EXTENSIONS


@dataclass
class ExtractResult:
    sample_id: Optional[object] = None
    reagent_lot_id: Optional[object] = None
    start_time: Optional[object] = None


def normalize_cell(x: object) -> str:
    """Normalize for label matching (case-insensitive, trimmed)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip().casefold()


def value_in_column_b(df: pd.DataFrame, row_index: int) -> Optional[object]:
    """
    Return the value in Column B (index 1) for the given row.
    Columns beyond B are ignored by design.
    """
    if df.shape[1] < 2:
        return None
    val = df.iat[row_index, 1]
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def load_workbook_like(path: str) -> List[Tuple[str, pd.DataFrame]]:
    """
    Return a list of (sheet_name, DataFrame) pairs for a given file path.
    - Excel -> all sheets
    - CSV   -> single pseudo-sheet named 'CSV'

    IMPORTANT: Each DataFrame is trimmed to only columns A:B (first two columns).
    """
    lower = path.lower()

    if lower.endswith(EXCEL_EXTENSIONS):
        sheets = pd.read_excel(path, sheet_name=None, header=None, engine="openpyxl")
        trimmed = []
        for name, df in sheets.items():
            if df is None or df.empty:
                trimmed.append((name, df))
            else:
                trimmed.append((name, df.iloc[:, :2]))  # keep only A:B
        return trimmed

    if lower.endswith(CSV_EXTENSIONS):
        # Robust CSV load: tolerate ragged rows / inconsistent field counts.
        # We read everything, then slice to A:B.
        read_kwargs = dict(
            header=None,
            dtype=object,
            engine="python",          # more tolerant than C engine
            encoding="utf-8-sig",     # handles BOM if present
        )

        try:
            df = pd.read_csv(path, **read_kwargs)
        except Exception:
            # If parsing still fails (very messy CSV), skip "bad" lines and continue.
            df = pd.read_csv(
                path,
                **read_kwargs,
                on_bad_lines="skip",   # pandas>=1.3
            )

        if df is not None and not df.empty:
            df = df.iloc[:, :2]        # keep only A:B
        return [("CSV", df)]

    raise ValueError(f"Unsupported file type: {path}")

def extract_entries_from_file(path: str) -> List[ExtractResult]:
    """
    Scan the input file (Excel or CSV) for repeated entries.

    Logic:
      - Scan Column A top-to-bottom.
      - When a target label appears in Column A, extract its value from Column B.
      - When all three targets are captured, append an ExtractResult and reset.
      - Continue scanning through all rows and all sheets.

    Notes:
      - Only complete entries are emitted.
      - Duplicate labels before completion overwrite the current value for that field.
      - Resets at each sheet boundary (does not carry partial entry across sheets).
    """
    targets_norm = {t: normalize_cell(t) for t in TARGET_FIELDS}
    entries: List[ExtractResult] = []

    for sheet_name, df in load_workbook_like(path):
        if df is None or df.empty or df.shape[1] < 2:
            continue

        current: Dict[str, object] = {}

        for r in range(len(df)):
            label = normalize_cell(df.iat[r, 0])
            if not label:
                continue

            matched_field = None
            for field, field_norm in targets_norm.items():
                if label == field_norm:
                    matched_field = field
                    break

            if matched_field is None:
                continue

            value = value_in_column_b(df, r)  # strictly Column B only
            current[matched_field] = value

            if all(f in current for f in TARGET_FIELDS):
                entries.append(
                    ExtractResult(
                        sample_id=current.get("Sample ID"),
                        reagent_lot_id=current.get("Reagent Lot ID"),
                        start_time=current.get("Start Time"),
                    )
                )
                current = {}

    return entries


def export_results(entries: List[ExtractResult], out_path: str) -> None:
    df = pd.DataFrame(
        [
            {
                "Sample ID": e.sample_id,
                "Reagent Lot ID": e.reagent_lot_id,
                "Start Time": e.start_time,
            }
            for e in entries
        ],
        columns=["Sample ID", "Reagent Lot ID", "Start Time"],
    )
    df.to_excel(out_path, index=False, engine="openpyxl")


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Field Extractor (Excel + CSV) — Column A/B Only")
        self.geometry("720x420")
        self.minsize(720, 420)

        self.input_file: Optional[str] = None
        self.output_path: Optional[str] = None

        self._build_ui()

    def _build_ui(self) -> None:
        pad = {"padx": 10, "pady": 8}

        top_frame = ttk.Frame(self)
        top_frame.pack(fill="x", **pad)

        ttk.Button(top_frame, text="Select Input File…", command=self.select_file).pack(side="left")
        ttk.Button(top_frame, text="Choose Output File…", command=self.choose_output).pack(side="left", padx=(10, 0))
        ttk.Button(top_frame, text="Run Extraction", command=self.run_extraction).pack(side="left", padx=(10, 0))

        mid_frame = ttk.Frame(self)
        mid_frame.pack(fill="both", expand=True, **pad)

        ttk.Label(mid_frame, text="Selected file:").pack(anchor="w")
        self.in_label = ttk.Label(mid_frame, text="(not selected)")
        self.in_label.pack(anchor="w", pady=(4, 10))

        out_frame = ttk.Frame(mid_frame)
        out_frame.pack(fill="x")

        ttk.Label(out_frame, text="Output:").pack(side="left")
        self.out_label = ttk.Label(out_frame, text="(not selected)")
        self.out_label.pack(side="left", padx=(8, 0))

        ttk.Label(mid_frame, text="Log:").pack(anchor="w", pady=(10, 0))
        self.log_text = tk.Text(mid_frame, height=12, wrap="word")
        self.log_text.pack(fill="both", expand=True, pady=(4, 0))

        note = (
            "Notes:\n"
            f"- Supports: {', '.join(SUPPORTED_EXTENSIONS)}\n"
            "- Labels must be in Column A; values must be in Column B.\n"
            "- Columns beyond B are ignored entirely.\n"
            "- Each output row = one complete set of Sample ID + Reagent Lot ID + Start Time."
        )
        ttk.Label(self, text=note, foreground="#444").pack(anchor="w", **pad)

    def log(self, msg: str) -> None:
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.update_idletasks()

    def select_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select an input file",
            filetypes=[
                ("Excel / CSV files", "*.xlsx *.xlsm *.xltx *.xltm *.csv"),
                ("Excel files", "*.xlsx *.xlsm *.xltx *.xltm"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return

        if not path.lower().endswith(SUPPORTED_EXTENSIONS):
            messagebox.showwarning("Unsupported file", "Please select a supported Excel or CSV file.")
            return

        self.input_file = path
        self.in_label.configure(text=path)
        self.log(f"Selected input: {os.path.basename(path)}")

    def choose_output(self) -> None:
        default_name = "extracted_entries.xlsx"
        out = filedialog.asksaveasfilename(
            title="Save output Excel file",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel file", "*.xlsx")],
        )
        if not out:
            return

        self.output_path = out
        self.out_label.configure(text=out)
        self.log(f"Output set to: {out}")

    def run_extraction(self) -> None:
        if not self.input_file:
            messagebox.showwarning("Missing input", "Please select an input file.")
            return
        if not self.output_path:
            messagebox.showwarning("Missing output", "Please choose an output file path.")
            return

        self.log("Starting extraction...")
        try:
            entries = extract_entries_from_file(self.input_file)
            self.log(f"Found {len(entries)} complete entr{'y' if len(entries)==1 else 'ies'}.")

            if len(entries) == 0:
                messagebox.showinfo(
                    "No entries found",
                    "No complete entries were found.\n\n"
                    "Make sure labels are in Column A and values are in Column B.",
                )
                return

            export_results(entries, self.output_path)
            self.log("Saved output successfully.")
            messagebox.showinfo("Success", f"Saved output:\n{self.output_path}")

        except Exception as e:
            tb = traceback.format_exc()
            self.log("ERROR:\n" + tb)
            messagebox.showerror("Error", f"An error occurred:\n{e}\n\nSee log for details.")


def main() -> int:
    app = App()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
