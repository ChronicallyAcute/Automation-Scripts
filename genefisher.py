"""
GeneFisher — search unannotated whole-genome FASTA files with local BLAST+,
then export selected hits as a multi-FASTA ready for MEGA.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from PyQt5.QtCore import (
    QObject, QThread, Qt, pyqtSignal, pyqtSlot,
)
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QAbstractItemView, QApplication, QFileDialog, QFrame,
    QGroupBox, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QMainWindow, QMessageBox, QProgressBar, QPushButton,
    QScrollArea, QSizePolicy, QSpinBox, QDoubleSpinBox,
    QComboBox, QStatusBar, QTableWidget, QTableWidgetItem,
    QToolButton, QVBoxLayout, QWidget,
)

# ── constants ────────────────────────────────────────────────────────────────
CONFIG_FILE = Path(__file__).parent / "config.json"
BLAST_DOWNLOAD_URL = "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/"
GENOME_EXTS = {".fna", ".fa", ".fasta"}
BLAST_OUTFMT = "6 qseqid sseqid pident length qstart qend sstart send evalue bitscore"
BLAST_COLS = ["Query", "Subject", "% Identity", "Length",
              "Q.Start", "Q.End", "S.Start", "S.End", "E-value", "Bitscore"]

STYLESHEET = """
QWidget {
    background-color: #f5f6f8;
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 11pt;
    color: #1e2128;
}
QGroupBox {
    font-weight: bold;
    font-size: 11pt;
    border: 1px solid #c8ccd4;
    border-radius: 6px;
    margin-top: 10px;
    padding-top: 10px;
    background-color: #ffffff;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
    color: #2c5282;
}
QPushButton {
    background-color: #3182ce;
    color: #ffffff;
    border: none;
    border-radius: 4px;
    padding: 6px 16px;
    font-size: 11pt;
}
QPushButton:hover {
    background-color: #2b6cb0;
}
QPushButton:pressed {
    background-color: #2c5282;
}
QPushButton:disabled {
    background-color: #a0aec0;
    color: #e2e8f0;
}
QPushButton#secondary {
    background-color: #e2e8f0;
    color: #2d3748;
    border: 1px solid #cbd5e0;
}
QPushButton#secondary:hover {
    background-color: #cbd5e0;
}
QPushButton#secondary:disabled {
    background-color: #edf2f7;
    color: #a0aec0;
}
QToolButton {
    background-color: transparent;
    border: none;
    font-size: 11pt;
    color: #3182ce;
    padding: 2px 4px;
}
QToolButton:hover {
    color: #2b6cb0;
    text-decoration: underline;
}
QLineEdit {
    border: 1px solid #cbd5e0;
    border-radius: 4px;
    padding: 4px 8px;
    background-color: #ffffff;
}
QLineEdit:focus {
    border: 1px solid #3182ce;
}
QSpinBox, QDoubleSpinBox, QComboBox {
    border: 1px solid #cbd5e0;
    border-radius: 4px;
    padding: 4px 6px;
    background-color: #ffffff;
}
QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {
    border: 1px solid #3182ce;
}
QTableWidget {
    border: 1px solid #e2e8f0;
    gridline-color: #edf2f7;
    background-color: #ffffff;
    alternate-background-color: #f7fafc;
}
QHeaderView::section {
    background-color: #ebf4ff;
    color: #2c5282;
    border: none;
    border-right: 1px solid #bee3f8;
    padding: 6px 8px;
    font-weight: bold;
}
QProgressBar {
    border: 1px solid #cbd5e0;
    border-radius: 4px;
    background-color: #e2e8f0;
    text-align: center;
    height: 14px;
}
QProgressBar::chunk {
    background-color: #3182ce;
    border-radius: 3px;
}
QStatusBar {
    background-color: #2c5282;
    color: #ebf8ff;
    font-size: 10pt;
}
QLabel#sectionLabel {
    color: #4a5568;
    font-size: 10pt;
}
QFrame#advancedFrame {
    border: 1px solid #e2e8f0;
    border-radius: 4px;
    background-color: #f7fafc;
}
"""

# ── helpers ──────────────────────────────────────────────────────────────────

def blast_available() -> dict[str, bool]:
    tools = ["blastn", "makeblastdb", "blastdbcmd"]
    return {t: shutil.which(t) is not None for t in tools}


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {}


def save_config(cfg: dict) -> None:
    try:
        CONFIG_FILE.write_text(json.dumps(cfg, indent=2))
    except Exception:
        pass


def folder_mtime(folder: str) -> float:
    total = 0.0
    for f in Path(folder).iterdir():
        if f.suffix.lower() in GENOME_EXTS:
            total += f.stat().st_mtime
    return total


# ── workers ──────────────────────────────────────────────────────────────────

class DbBuildWorker(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, folder: str):
        super().__init__()
        self.folder = folder

    @pyqtSlot()
    def run(self):
        try:
            folder = Path(self.folder)
            fasta_files = [f for f in folder.iterdir()
                           if f.suffix.lower() in GENOME_EXTS]
            if not fasta_files:
                self.error.emit(
                    f"No genome FASTA files (.fna/.fa/.fasta) found in:\n{self.folder}"
                )
                return

            db_dir = folder / ".blastdb"
            db_dir.mkdir(exist_ok=True)
            mtime_file = db_dir / "mtime.txt"
            db_prefix = str(db_dir / "genomes")
            current_mtime = folder_mtime(self.folder)

            if mtime_file.exists():
                try:
                    cached = float(mtime_file.read_text())
                    if abs(cached - current_mtime) < 0.01:
                        self.status.emit("BLAST database is up-to-date — skipping rebuild.")
                        self.finished.emit()
                        return
                except Exception:
                    pass

            self.status.emit(f"Concatenating {len(fasta_files)} genome file(s)…")
            combined = db_dir / "combined.fasta"
            with combined.open("w") as out:
                for fasta in sorted(fasta_files):
                    out.write(fasta.read_text())

            self.status.emit("Running makeblastdb…")
            cmd = [
                "makeblastdb",
                "-in", str(combined),
                "-dbtype", "nucl",
                "-out", db_prefix,
                "-parse_seqids",
                "-title", "GeneFisher_DB",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                self.error.emit(
                    "makeblastdb failed:\n" + (result.stderr or result.stdout)
                )
                return

            mtime_file.write_text(str(current_mtime))
            self.status.emit("BLAST database built successfully.")
            self.finished.emit()
        except Exception as exc:
            self.error.emit(f"Database build error:\n{exc}")


class BlastWorker(QObject):
    finished = pyqtSignal(list)   # list of row dicts
    error = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, folder: str, query: str, params: dict):
        super().__init__()
        self.folder = folder
        self.query = query
        self.params = params

    @pyqtSlot()
    def run(self):
        try:
            db_prefix = str(Path(self.folder) / ".blastdb" / "genomes")
            p = self.params

            self.status.emit("Running BLAST search…")
            cmd = [
                "blastn",
                "-db", db_prefix,
                "-query", self.query,
                "-task", p.get("task", "blastn"),
                "-perc_identity", str(p.get("perc_identity", 70)),
                "-evalue", str(p.get("evalue", 1e-10)),
                "-max_hsps", str(p.get("max_hits", 1)),
                "-num_threads", str(p.get("threads", os.cpu_count() or 1)),
                "-outfmt", BLAST_OUTFMT,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                self.error.emit("BLAST failed:\n" + (result.stderr or result.stdout))
                return

            rows = []
            for line in result.stdout.strip().splitlines():
                parts = line.split("\t")
                if len(parts) == 10:
                    rows.append({
                        "qseqid":   parts[0],
                        "sseqid":   parts[1],
                        "pident":   float(parts[2]),
                        "length":   int(parts[3]),
                        "qstart":   int(parts[4]),
                        "qend":     int(parts[5]),
                        "sstart":   int(parts[6]),
                        "send":     int(parts[7]),
                        "evalue":   parts[8],
                        "bitscore": parts[9],
                    })

            if not rows:
                self.status.emit("BLAST finished — no hits found.")
            else:
                self.status.emit(f"BLAST finished — {len(rows)} hit(s) found.")
            self.finished.emit(rows)
        except Exception as exc:
            self.error.emit(f"BLAST error:\n{exc}")


class ExtractWorker(QObject):
    finished = pyqtSignal(str)   # output FASTA path
    error = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, folder: str, rows: list[dict], out_path: str):
        super().__init__()
        self.folder = folder
        self.rows = rows
        self.out_path = out_path

    @pyqtSlot()
    def run(self):
        try:
            db_prefix = str(Path(self.folder) / ".blastdb" / "genomes")
            sequences = []

            for row in self.rows:
                sseqid = row["sseqid"]
                sstart = row["sstart"]
                send   = row["send"]
                pident = row["pident"]

                # blastdbcmd always wants start < end; track strand separately
                strand = "plus"
                s, e = sstart, send
                if sstart > send:
                    strand = "minus"
                    s, e = send, sstart

                self.status.emit(f"Extracting {sseqid} {s}-{e}…")
                cmd = [
                    "blastdbcmd",
                    "-db", db_prefix,
                    "-entry", sseqid,
                    "-range", f"{s}-{e}",
                    "-strand", strand,
                    "-outfmt", "%s",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    self.error.emit(
                        f"blastdbcmd failed for {sseqid}:\n"
                        + (result.stderr or result.stdout)
                    )
                    return

                seq = result.stdout.strip().replace("\n", "")
                header = f">{sseqid}|{pident:.1f}pct|{sstart}-{send}"
                sequences.append(f"{header}\n{seq}")

            with open(self.out_path, "w") as fh:
                fh.write("\n".join(sequences) + "\n")

            self.status.emit(
                f"Exported {len(sequences)} sequence(s) to {Path(self.out_path).name}."
            )
            self.finished.emit(self.out_path)
        except Exception as exc:
            self.error.emit(f"Extraction error:\n{exc}")


# ── main window ──────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GeneFisher")
        self.setMinimumWidth(920)
        self.setMinimumHeight(700)

        self._config = load_config()
        self._blast_ok = blast_available()
        self._blast_rows: list[dict] = []
        self._db_thread: QThread | None = None
        self._blast_thread: QThread | None = None
        self._extract_thread: QThread | None = None

        self._build_ui()
        self._apply_blast_status()
        self._restore_config()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self):
        self.setStyleSheet(STYLESHEET)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 6)
        root.setSpacing(10)

        root.addWidget(self._build_step1())
        root.addWidget(self._build_step2())
        root.addWidget(self._build_step3())

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready.")

    # -- Step 1 ----------------------------------------------------------------

    def _build_step1(self) -> QGroupBox:
        box = QGroupBox("Step 1 — Genome Folder & BLAST Database")
        lay = QVBoxLayout(box)
        lay.setSpacing(8)

        row = QHBoxLayout()
        self.genome_folder_edit = QLineEdit()
        self.genome_folder_edit.setPlaceholderText("Select folder containing .fna / .fa / .fasta files…")
        self.genome_folder_edit.setReadOnly(True)
        browse_btn = QPushButton("Browse…")
        browse_btn.setObjectName("secondary")
        browse_btn.setFixedWidth(100)
        browse_btn.clicked.connect(self._browse_genome_folder)
        row.addWidget(self.genome_folder_edit)
        row.addWidget(browse_btn)
        lay.addLayout(row)

        row2 = QHBoxLayout()
        self.build_db_btn = QPushButton("Build BLAST Database")
        self.build_db_btn.clicked.connect(self._build_db)
        self.db_progress = QProgressBar()
        self.db_progress.setRange(0, 0)
        self.db_progress.setVisible(False)
        self.db_progress.setFixedHeight(14)
        row2.addWidget(self.build_db_btn)
        row2.addWidget(self.db_progress, 1)
        lay.addLayout(row2)

        return box

    # -- Step 2 ----------------------------------------------------------------

    def _build_step2(self) -> QGroupBox:
        box = QGroupBox("Step 2 — Query & BLAST Parameters")
        lay = QVBoxLayout(box)
        lay.setSpacing(8)

        # query file row
        qrow = QHBoxLayout()
        self.query_edit = QLineEdit()
        self.query_edit.setPlaceholderText("Select query FASTA file…")
        self.query_edit.setReadOnly(True)
        qbrowse = QPushButton("Browse…")
        qbrowse.setObjectName("secondary")
        qbrowse.setFixedWidth(100)
        qbrowse.clicked.connect(self._browse_query)
        qrow.addWidget(self.query_edit)
        qrow.addWidget(qbrowse)
        lay.addLayout(qrow)

        # basic params row
        params_row = QHBoxLayout()
        params_row.setSpacing(16)

        params_row.addWidget(QLabel("Task:"))
        self.task_combo = QComboBox()
        self.task_combo.addItems(["blastn", "megablast", "dc-megablast"])
        self.task_combo.setFixedWidth(130)
        params_row.addWidget(self.task_combo)

        params_row.addWidget(QLabel("% Identity:"))
        self.pident_spin = QSpinBox()
        self.pident_spin.setRange(50, 100)
        self.pident_spin.setValue(70)
        self.pident_spin.setSuffix("%")
        self.pident_spin.setFixedWidth(80)
        params_row.addWidget(self.pident_spin)

        params_row.addWidget(QLabel("E-value:"))
        self.evalue_combo = QComboBox()
        self.evalue_combo.addItems(["1e-3", "1e-5", "1e-10", "1e-20", "1e-50"])
        self.evalue_combo.setCurrentText("1e-10")
        self.evalue_combo.setEditable(True)
        self.evalue_combo.setFixedWidth(90)
        params_row.addWidget(self.evalue_combo)

        params_row.addStretch()
        lay.addLayout(params_row)

        # advanced toggle
        self.adv_toggle = QToolButton()
        self.adv_toggle.setText("▶ Advanced Parameters")
        self.adv_toggle.setCheckable(True)
        self.adv_toggle.setChecked(False)
        self.adv_toggle.clicked.connect(self._toggle_advanced)
        lay.addWidget(self.adv_toggle)

        # advanced frame
        self.adv_frame = QFrame()
        self.adv_frame.setObjectName("advancedFrame")
        self.adv_frame.setVisible(False)
        adv_lay = QHBoxLayout(self.adv_frame)
        adv_lay.setSpacing(16)

        adv_lay.addWidget(QLabel("Max hits / subject:"))
        self.maxhits_spin = QSpinBox()
        self.maxhits_spin.setRange(1, 100)
        self.maxhits_spin.setValue(1)
        self.maxhits_spin.setFixedWidth(70)
        adv_lay.addWidget(self.maxhits_spin)

        adv_lay.addWidget(QLabel("Threads:"))
        self.threads_spin = QSpinBox()
        self.threads_spin.setRange(1, os.cpu_count() or 1)
        self.threads_spin.setValue(os.cpu_count() or 1)
        self.threads_spin.setFixedWidth(70)
        adv_lay.addWidget(self.threads_spin)

        adv_lay.addStretch()
        lay.addWidget(self.adv_frame)

        # run button + progress
        run_row = QHBoxLayout()
        self.run_blast_btn = QPushButton("Run BLAST")
        self.run_blast_btn.clicked.connect(self._run_blast)
        self.blast_progress = QProgressBar()
        self.blast_progress.setRange(0, 0)
        self.blast_progress.setVisible(False)
        self.blast_progress.setFixedHeight(14)
        run_row.addWidget(self.run_blast_btn)
        run_row.addWidget(self.blast_progress, 1)
        lay.addLayout(run_row)

        return box

    # -- Step 3 ----------------------------------------------------------------

    def _build_step3(self) -> QGroupBox:
        box = QGroupBox("Step 3 — Results & Export")
        box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        lay = QVBoxLayout(box)
        lay.setSpacing(8)

        ctrl_row = QHBoxLayout()
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.setObjectName("secondary")
        self.select_all_btn.clicked.connect(self._select_all)
        self.deselect_all_btn = QPushButton("Deselect All")
        self.deselect_all_btn.setObjectName("secondary")
        self.deselect_all_btn.clicked.connect(self._deselect_all)
        self.hit_count_label = QLabel("No results.")
        self.hit_count_label.setObjectName("sectionLabel")
        ctrl_row.addWidget(self.select_all_btn)
        ctrl_row.addWidget(self.deselect_all_btn)
        ctrl_row.addStretch()
        ctrl_row.addWidget(self.hit_count_label)
        lay.addLayout(ctrl_row)

        self.results_table = QTableWidget(0, len(BLAST_COLS))
        self.results_table.setHorizontalHeaderLabels(BLAST_COLS)
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setSelectionMode(QAbstractItemView.MultiSelection)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        lay.addWidget(self.results_table, 1)

        self.export_btn = QPushButton("Export Selected Hits to FASTA")
        self.export_btn.clicked.connect(self._export_fasta)
        lay.addWidget(self.export_btn)

        return box

    # ── config / startup ─────────────────────────────────────────────────────

    def _apply_blast_status(self):
        missing = [k for k, v in self._blast_ok.items() if not v]
        if missing:
            QMessageBox.warning(
                self,
                "BLAST+ Not Found",
                f"The following BLAST+ program(s) were not found on your PATH:\n\n"
                f"  {', '.join(missing)}\n\n"
                f"Please install NCBI BLAST+ and make sure it is on your system PATH.\n\n"
                f"Download from:\n{BLAST_DOWNLOAD_URL}",
            )
            self.build_db_btn.setEnabled(False)
            self.run_blast_btn.setEnabled(False)
            self.export_btn.setEnabled(False)
            self.status_bar.showMessage(
                "BLAST+ not found — please install BLAST+ and restart."
            )

    def _restore_config(self):
        cfg = self._config
        if "genome_folder" in cfg:
            self.genome_folder_edit.setText(cfg["genome_folder"])
        if "query_path" in cfg:
            self.query_edit.setText(cfg["query_path"])
        if "task" in cfg:
            idx = self.task_combo.findText(cfg["task"])
            if idx >= 0:
                self.task_combo.setCurrentIndex(idx)
        if "perc_identity" in cfg:
            self.pident_spin.setValue(cfg["perc_identity"])
        if "evalue" in cfg:
            self.evalue_combo.setCurrentText(cfg["evalue"])
        if "max_hits" in cfg:
            self.maxhits_spin.setValue(cfg["max_hits"])
        if "threads" in cfg:
            t = cfg["threads"]
            self.threads_spin.setValue(min(t, self.threads_spin.maximum()))

    def _persist_config(self):
        self._config.update({
            "genome_folder":  self.genome_folder_edit.text(),
            "query_path":     self.query_edit.text(),
            "task":           self.task_combo.currentText(),
            "perc_identity":  self.pident_spin.value(),
            "evalue":         self.evalue_combo.currentText(),
            "max_hits":       self.maxhits_spin.value(),
            "threads":        self.threads_spin.value(),
        })
        save_config(self._config)

    def closeEvent(self, event):
        self._persist_config()
        super().closeEvent(event)

    # ── slots ────────────────────────────────────────────────────────────────

    def _browse_genome_folder(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select Genome Folder",
            self.genome_folder_edit.text() or str(Path.home()),
        )
        if folder:
            self.genome_folder_edit.setText(folder)
            self._persist_config()

    def _browse_query(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Query FASTA",
            self.query_edit.text() or str(Path.home()),
            "FASTA files (*.fna *.fa *.fasta *.fas);;All files (*)",
        )
        if path:
            self.query_edit.setText(path)
            self._persist_config()

    def _toggle_advanced(self, checked: bool):
        self.adv_frame.setVisible(checked)
        self.adv_toggle.setText(
            ("▼" if checked else "▶") + " Advanced Parameters"
        )

    def _select_all(self):
        self.results_table.selectAll()

    def _deselect_all(self):
        self.results_table.clearSelection()

    # ── database build ───────────────────────────────────────────────────────

    def _build_db(self):
        folder = self.genome_folder_edit.text().strip()
        if not folder:
            QMessageBox.critical(
                self, "No Folder Selected",
                "Please select a genome folder before building the database."
            )
            return

        if not Path(folder).is_dir():
            QMessageBox.critical(
                self, "Folder Not Found",
                f"The selected folder does not exist:\n{folder}"
            )
            return

        self._set_db_busy(True)
        self.status_bar.showMessage("Building BLAST database…")

        worker = DbBuildWorker(folder)
        thread = QThread(self)
        worker.moveToThread(thread)

        thread.started.connect(worker.run)
        worker.status.connect(self._on_db_status)
        worker.finished.connect(self._on_db_finished)
        worker.error.connect(self._on_db_error)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)

        self._db_thread = thread
        self._db_worker = worker
        thread.start()

    @pyqtSlot(str)
    def _on_db_status(self, msg: str):
        self.status_bar.showMessage(msg)

    @pyqtSlot()
    def _on_db_finished(self):
        self._set_db_busy(False)

    @pyqtSlot(str)
    def _on_db_error(self, msg: str):
        self._set_db_busy(False)
        QMessageBox.critical(self, "Database Build Failed", msg)
        self.status_bar.showMessage("Database build failed.")

    def _set_db_busy(self, busy: bool):
        self.build_db_btn.setEnabled(not busy)
        self.db_progress.setVisible(busy)

    # ── BLAST run ─────────────────────────────────────────────────────────────

    def _run_blast(self):
        folder = self.genome_folder_edit.text().strip()
        query  = self.query_edit.text().strip()

        if not folder:
            QMessageBox.critical(self, "No Genome Folder",
                                  "Please select a genome folder first.")
            return
        if not query:
            QMessageBox.critical(self, "No Query File",
                                  "Please select a query FASTA file first.")
            return
        if not Path(folder).is_dir():
            QMessageBox.critical(self, "Folder Not Found",
                                  f"Genome folder not found:\n{folder}")
            return
        if not Path(query).is_file():
            QMessageBox.critical(self, "Query File Not Found",
                                  f"Query file not found:\n{query}")
            return

        db_nhr = Path(folder) / ".blastdb" / "genomes.nhr"
        if not db_nhr.exists():
            QMessageBox.critical(
                self, "Database Not Built",
                "Please build the BLAST database first (Step 1)."
            )
            return

        self._persist_config()
        params = {
            "task":         self.task_combo.currentText(),
            "perc_identity": self.pident_spin.value(),
            "evalue":       self.evalue_combo.currentText(),
            "max_hits":     self.maxhits_spin.value(),
            "threads":      self.threads_spin.value(),
        }

        self._set_blast_busy(True)
        self.status_bar.showMessage("Running BLAST search…")
        self.results_table.setRowCount(0)
        self._blast_rows = []

        worker = BlastWorker(folder, query, params)
        thread = QThread(self)
        worker.moveToThread(thread)

        thread.started.connect(worker.run)
        worker.status.connect(self._on_blast_status)
        worker.finished.connect(self._on_blast_finished)
        worker.error.connect(self._on_blast_error)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)

        self._blast_thread = thread
        self._blast_worker = worker
        thread.start()

    @pyqtSlot(str)
    def _on_blast_status(self, msg: str):
        self.status_bar.showMessage(msg)

    @pyqtSlot(list)
    def _on_blast_finished(self, rows: list):
        self._set_blast_busy(False)
        self._blast_rows = rows
        self._populate_table(rows)

    @pyqtSlot(str)
    def _on_blast_error(self, msg: str):
        self._set_blast_busy(False)
        QMessageBox.critical(self, "BLAST Search Failed", msg)
        self.status_bar.showMessage("BLAST search failed.")

    def _set_blast_busy(self, busy: bool):
        self.run_blast_btn.setEnabled(not busy)
        self.blast_progress.setVisible(busy)

    # ── table population ──────────────────────────────────────────────────────

    def _populate_table(self, rows: list[dict]):
        self.results_table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            vals = [
                row["qseqid"], row["sseqid"],
                f"{row['pident']:.1f}",
                str(row["length"]),
                str(row["qstart"]), str(row["qend"]),
                str(row["sstart"]), str(row["send"]),
                row["evalue"], row["bitscore"],
            ]
            pident = row["pident"]
            if pident >= 95:
                bg = QColor("#c6f6d5")   # green
            elif pident >= 80:
                bg = QColor("#fefcbf")   # yellow
            else:
                bg = QColor("#fed7aa")   # orange

            for c, val in enumerate(vals):
                item = QTableWidgetItem(val)
                item.setBackground(bg)
                self.results_table.setItem(r, c, item)

        n = len(rows)
        self.hit_count_label.setText(
            f"{n} hit{'s' if n != 1 else ''} found." if n else "No hits found."
        )

    # ── export ────────────────────────────────────────────────────────────────

    def _export_fasta(self):
        selected_rows = sorted(set(
            idx.row() for idx in self.results_table.selectedIndexes()
        ))
        if not selected_rows:
            QMessageBox.information(
                self, "No Rows Selected",
                "Please select one or more rows in the results table to export."
            )
            return

        folder = self.genome_folder_edit.text().strip()
        if not folder:
            QMessageBox.critical(self, "No Genome Folder",
                                  "Genome folder is not set.")
            return

        out_path, _ = QFileDialog.getSaveFileName(
            self, "Save FASTA",
            str(Path.home() / "genefisher_hits.fasta"),
            "FASTA files (*.fasta *.fa);;All files (*)",
        )
        if not out_path:
            return

        rows_to_export = [self._blast_rows[i] for i in selected_rows]

        self.export_btn.setEnabled(False)
        self.status_bar.showMessage("Extracting sequences…")

        worker = ExtractWorker(folder, rows_to_export, out_path)
        thread = QThread(self)
        worker.moveToThread(thread)

        thread.started.connect(worker.run)
        worker.status.connect(self._on_extract_status)
        worker.finished.connect(self._on_extract_finished)
        worker.error.connect(self._on_extract_error)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)

        self._extract_thread = thread
        self._extract_worker = worker
        thread.start()

    @pyqtSlot(str)
    def _on_extract_status(self, msg: str):
        self.status_bar.showMessage(msg)

    @pyqtSlot(str)
    def _on_extract_finished(self, path: str):
        self.export_btn.setEnabled(True)
        QMessageBox.information(
            self, "Export Complete",
            f"Selected sequences exported to:\n{path}"
        )

    @pyqtSlot(str)
    def _on_extract_error(self, msg: str):
        self.export_btn.setEnabled(True)
        QMessageBox.critical(self, "Export Failed", msg)
        self.status_bar.showMessage("Export failed.")


# ── entry point ──────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("GeneFisher")
    app.setApplicationDisplayName("GeneFisher")
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
