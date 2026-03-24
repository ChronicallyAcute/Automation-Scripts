import sys
import os
import shutil
import csv
import re
from datetime import datetime

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QFileDialog, QMessageBox,
    QTextEdit, QTableWidget, QTableWidgetItem,
    QProgressBar, QStackedWidget, QListWidget,
    QListWidgetItem, QCheckBox, QStackedLayout,
    QGraphicsOpacityEffect, QSizePolicy
)
from PyQt5.QtCore import QStandardPaths, Qt, QSize
from PyQt5.QtGui import QKeySequence, QGuiApplication, QPixmap


BPR_FOLDER_NAME = "Scanned Batch Production Records (BPR)"
LOT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
PCR_ROOT_DEFAULT = r"T:\PCR\++COMPLETED-SUBMITTED PCR DATA and FORMS"
LOGO_FILENAME = "alembic_logo.png"
PART_COL_WIDTH = 440
LOT_COL_WIDTH = 160


# -------------------- HELPERS --------------------

def resource_path(relative_path):
    base_dir = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base_dir, relative_path)

def extract_6digit_lot(filename):
    """
    Extract exactly one 6-digit lot number from filename.
    Returns int if found, otherwise None.
    """
    match = re.search(r"\b(\d{6})\b", filename)
    if match:
        return int(match.group(1))
    return None


def make_dest_filename(original_name, lot):
    if not lot:
        return original_name
    prefix = f"{lot} "
    if original_name.startswith(prefix):
        return original_name
    return f"{prefix}{original_name}"

def make_bpr_output_filename(part_number, lot_number):
    if not lot_number:
        return ""
    if part_number:
        return f"{part_number} {lot_number}.pdf"
    return f"{lot_number}.pdf"


# -------------------- TABLE WITH EXCEL PASTE --------------------

class ScaledLogoLabel(QLabel):
    def __init__(self, pixmap, max_height=200, parent=None):
        super().__init__(parent)
        self._source_pixmap = pixmap
        self.setAlignment(Qt.AlignCenter)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setMaximumHeight(max_height)
        self._update_pixmap()

    def resizeEvent(self, event):
        self._update_pixmap()
        super().resizeEvent(event)

    def sizeHint(self):
        return QSize(240, 140)

    def _update_pixmap(self):
        if self._source_pixmap is None or self._source_pixmap.isNull():
            self.clear()
            return

        target = self.size()
        if target.width() <= 0 or target.height() <= 0:
            return

        scaled = self._source_pixmap.scaled(
            target,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation
        )
        self.setPixmap(scaled)


class ExcelPasteTable(QTableWidget):
    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Paste):
            clipboard = QGuiApplication.clipboard()
            text = clipboard.text()
            if not text:
                return

            rows = text.splitlines()
            start_row = self.currentRow()
            start_col = self.currentColumn()

            for r, row_data in enumerate(rows):
                if start_row + r >= self.rowCount():
                    break

                cols = row_data.split("\t")
                for c, value in enumerate(cols):
                    if start_col + c >= self.columnCount():
                        break

                    self.setItem(
                        start_row + r,
                        start_col + c,
                        QTableWidgetItem(value.strip())
                    )
            return

        super().keyPressEvent(event)


# -------------------- MAIN APP --------------------

class PDFDownloader(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PDF Lot Downloader (GMP-Safe)")
        self.setMinimumSize(950, 720)

        self.source_root = r"S:\++MASTER PRODUCT DOCUMENTS"
        self.destination_root = self.get_default_downloads_dir()
        self.pcr_root = PCR_ROOT_DEFAULT

        self.output_dir = ""
        self.audit_log_path = ""

        self.init_ui()

    def get_default_downloads_dir(self):
        return QStandardPaths.writableLocation(QStandardPaths.DownloadLocation)

    def init_ui(self):
        main_layout = QVBoxLayout()

        mode_layout = QHBoxLayout()
        self.mode_bpr_btn = QPushButton("BPR Record Pull")
        self.mode_bpr_btn.clicked.connect(lambda: self.switch_mode(0))
        mode_layout.addWidget(self.mode_bpr_btn)

        self.mode_upload_btn = QPushButton("File Upload")
        self.mode_upload_btn.clicked.connect(lambda: self.switch_mode(1))
        mode_layout.addWidget(self.mode_upload_btn)
        main_layout.addLayout(mode_layout)

        self.stack = QStackedWidget()
        self.bpr_page = QWidget()
        self.upload_page = QWidget()

        self.init_bpr_ui(self.bpr_page)
        self.init_upload_ui(self.upload_page)

        self.stack.addWidget(self.bpr_page)
        self.stack.addWidget(self.upload_page)
        main_layout.addWidget(self.stack)

        self.setLayout(main_layout)
        self.switch_mode(0)

    def switch_mode(self, index):
        self.stack.setCurrentIndex(index)
        self.clear_inputs_for_mode(index)

    def clear_inputs_for_mode(self, index):
        if index == 0:
            self.bpr_table.clearContents()
            self.bpr_table.setCurrentCell(0, 0)
            return

        if index == 1:
            self.pcr_table.clearContents()
            self.pcr_table.setCurrentCell(0, 0)
            return

    def build_logo_label(self):
        logo_path = resource_path(LOGO_FILENAME)
        if not os.path.isfile(logo_path):
            return None

        pixmap = QPixmap(logo_path)
        if pixmap.isNull():
            return None

        logo_label = ScaledLogoLabel(pixmap)

        opacity = QGraphicsOpacityEffect(logo_label)
        opacity.setOpacity(0.3)
        logo_label.setGraphicsEffect(opacity)
        return logo_label

    def create_log_panel(self):
        log_edit = QTextEdit()
        log_edit.setReadOnly(True)
        log_edit.setFixedHeight(160)
        log_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        log_edit.setStyleSheet(
            "QTextEdit { background-color: rgba(255, 255, 255, 230); }"
        )

        logo_label = self.build_logo_label()
        if logo_label is None:
            return log_edit, log_edit

        container = QWidget()
        stack = QStackedLayout(container)
        stack.setStackingMode(QStackedLayout.StackAll)
        stack.addWidget(logo_label)
        stack.addWidget(log_edit)
        return container, log_edit

    def init_bpr_ui(self, parent):
        layout = QVBoxLayout()

        layout.addWidget(QLabel(
            "Paste Part Numbers and Lot Numbers directly from Excel:"
        ))

        # ---- TABLE ----
        self.bpr_table = ExcelPasteTable(30, 2)
        self.bpr_table.setHorizontalHeaderLabels(["Part Number", "Lot Number"])
        self.bpr_table.horizontalHeader().setStretchLastSection(True)
        self.bpr_table.verticalHeader().setVisible(False)
        self.bpr_table.setColumnWidth(0, PART_COL_WIDTH)
        self.bpr_table.setColumnWidth(1, LOT_COL_WIDTH)
        layout.addWidget(self.bpr_table)

        # ---- FOLDER BUTTONS ----
        folder_layout = QHBoxLayout()

        self.source_btn = QPushButton("Select Source Root Folder")
        self.source_btn.clicked.connect(self.select_source_root)
        folder_layout.addWidget(self.source_btn)

        self.dest_btn = QPushButton("Select Destination Folder")
        self.dest_btn.clicked.connect(self.select_destination)
        folder_layout.addWidget(self.dest_btn)

        layout.addLayout(folder_layout)

        self.destination_label = QLabel()
        self.update_destination_label()
        layout.addWidget(self.destination_label)

        # ---- RUN ----
        self.run_btn = QPushButton("Download PDFs")
        self.run_btn.clicked.connect(self.process_files)
        layout.addWidget(self.run_btn)

        # ---- PROGRESS BAR ----
        self.progress = QProgressBar()
        self.progress.setMinimum(0)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        layout.addWidget(self.progress)

        # ---- LOG ----
        layout.addWidget(QLabel("Log:"))
        log_container, self.log = self.create_log_panel()
        layout.addWidget(log_container)

        parent.setLayout(layout)

    def init_upload_ui(self, parent):
        layout = QVBoxLayout()

        layout.addWidget(QLabel("Select files to copy:"))

        files_btn_layout = QHBoxLayout()
        self.add_files_btn = QPushButton("Add Files")
        self.add_files_btn.clicked.connect(self.add_files)
        files_btn_layout.addWidget(self.add_files_btn)

        self.check_all_files_btn = QPushButton("Check All")
        self.check_all_files_btn.clicked.connect(
            lambda: self.set_all_check_state(self.files_list, Qt.Checked)
        )
        files_btn_layout.addWidget(self.check_all_files_btn)

        self.uncheck_all_files_btn = QPushButton("Uncheck All")
        self.uncheck_all_files_btn.clicked.connect(
            lambda: self.set_all_check_state(self.files_list, Qt.Unchecked)
        )
        files_btn_layout.addWidget(self.uncheck_all_files_btn)

        self.clear_files_btn = QPushButton("Clear Files")
        self.clear_files_btn.clicked.connect(self.clear_files)
        files_btn_layout.addWidget(self.clear_files_btn)

        layout.addLayout(files_btn_layout)

        self.files_list = QListWidget()
        layout.addWidget(self.files_list)

        layout.addWidget(QLabel("Destination folders:"))

        folders_btn_layout = QHBoxLayout()
        self.add_folders_btn = QPushButton("Add Folder")
        self.add_folders_btn.clicked.connect(self.add_folders)
        folders_btn_layout.addWidget(self.add_folders_btn)

        self.check_all_folders_btn = QPushButton("Check All")
        self.check_all_folders_btn.clicked.connect(
            lambda: self.set_all_check_state(self.folders_list, Qt.Checked)
        )
        folders_btn_layout.addWidget(self.check_all_folders_btn)

        self.uncheck_all_folders_btn = QPushButton("Uncheck All")
        self.uncheck_all_folders_btn.clicked.connect(
            lambda: self.set_all_check_state(self.folders_list, Qt.Unchecked)
        )
        folders_btn_layout.addWidget(self.uncheck_all_folders_btn)

        self.clear_folders_btn = QPushButton("Clear Folders")
        self.clear_folders_btn.clicked.connect(self.clear_folders)
        folders_btn_layout.addWidget(self.clear_folders_btn)

        layout.addLayout(folders_btn_layout)

        self.folders_list = QListWidget()
        layout.addWidget(self.folders_list)

        self.use_pcr_checkbox = QCheckBox(
            "Use PCR default destinations (part/lot pairs)"
        )
        self.use_pcr_checkbox.toggled.connect(self.toggle_pcr_options)
        layout.addWidget(self.use_pcr_checkbox)

        self.pcr_root_label = QLabel(f"PCR root:\n{self.pcr_root}")
        layout.addWidget(self.pcr_root_label)

        self.pcr_table_label = QLabel(
            "Paste Part Numbers and Lot Numbers for PCR destinations:"
        )
        layout.addWidget(self.pcr_table_label)

        self.pcr_table = ExcelPasteTable(20, 2)
        self.pcr_table.setHorizontalHeaderLabels(["Part Number", "Lot Number"])
        self.pcr_table.horizontalHeader().setStretchLastSection(True)
        self.pcr_table.verticalHeader().setVisible(False)
        self.pcr_table.setColumnWidth(0, PART_COL_WIDTH)
        self.pcr_table.setColumnWidth(1, LOT_COL_WIDTH)
        layout.addWidget(self.pcr_table)

        self.toggle_pcr_options(False)

        self.upload_btn = QPushButton("Copy Files")
        self.upload_btn.clicked.connect(self.process_upload)
        layout.addWidget(self.upload_btn)

        self.upload_progress = QProgressBar()
        self.upload_progress.setMinimum(0)
        self.upload_progress.setValue(0)
        self.upload_progress.setTextVisible(True)
        layout.addWidget(self.upload_progress)

        layout.addWidget(QLabel("Log:"))
        upload_log_container, self.upload_log = self.create_log_panel()
        layout.addWidget(upload_log_container)

        parent.setLayout(layout)

    # -------------------- DIALOGS --------------------

    def select_source_root(self):
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.Directory)
        dialog.setOption(QFileDialog.ShowDirsOnly, True)

        if dialog.exec_():
            self.source_root = dialog.selectedFiles()[0]
            self.log.append(f"Source root set to: {self.source_root}")

    def select_destination(self):
        dialog = QFileDialog()
        dialog.setFileMode(QFileDialog.Directory)
        dialog.setOption(QFileDialog.ShowDirsOnly, True)

        if dialog.exec_():
            self.destination_root = dialog.selectedFiles()[0]
            self.log.append(f"Destination root set to: {self.destination_root}")
            self.update_destination_label()

    def update_destination_label(self):
        self.destination_label.setText(f"Destination:\n{self.destination_root}")

    # -------------------- FILE UPLOAD HELPERS --------------------

    def set_all_check_state(self, list_widget, state):
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            item.setCheckState(state)

    def get_checked_items(self, list_widget):
        items = []
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            if item.checkState() == Qt.Checked:
                items.append(item.data(Qt.UserRole))
        return items

    def add_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Files"
        )
        if not files:
            return

        existing = {
            self.files_list.item(i).data(Qt.UserRole)
            for i in range(self.files_list.count())
        }

        for path in files:
            if path in existing:
                continue
            item = QListWidgetItem(os.path.basename(path))
            item.setToolTip(path)
            item.setData(Qt.UserRole, path)
            item.setCheckState(Qt.Checked)
            self.files_list.addItem(item)

    def clear_files(self):
        self.files_list.clear()

    def add_folders(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select Destination Folder"
        )
        if not folder:
            return

        existing = {
            self.folders_list.item(i).data(Qt.UserRole)
            for i in range(self.folders_list.count())
        }

        if folder in existing:
            return

        item = QListWidgetItem(folder)
        item.setToolTip(folder)
        item.setData(Qt.UserRole, folder)
        item.setCheckState(Qt.Checked)
        self.folders_list.addItem(item)

    def clear_folders(self):
        self.folders_list.clear()

    def toggle_pcr_options(self, checked):
        self.pcr_root_label.setEnabled(checked)
        self.pcr_table_label.setEnabled(checked)
        self.pcr_table.setEnabled(checked)

    def build_pcr_destinations(self):
        destinations = []
        seen = set()

        for row in range(self.pcr_table.rowCount()):
            part_item = self.pcr_table.item(row, 0)
            if not part_item or not part_item.text().strip():
                continue

            part_number = part_item.text().strip()
            lot_item = self.pcr_table.item(row, 1)
            lot_number = lot_item.text().strip() if lot_item else ""

            if not lot_number:
                self.upload_log.append(
                    f"Skipped row {row + 1}: missing lot number for {part_number}"
                )
                continue

            if not LOT_PATTERN.match(lot_number):
                self.upload_log.append(
                    f"Skipped row {row + 1}: invalid lot number '{lot_number}'"
                )
                continue

            if len(part_number) < 3:
                self.upload_log.append(
                    f"Skipped row {row + 1}: part number too short '{part_number}'"
                )
                continue

            dest = os.path.join(
                self.pcr_root,
                part_number[:2],
                part_number[:3],
                part_number,
                lot_number,
                "Data"
            )

            key = (dest, lot_number)
            if key not in seen:
                destinations.append({
                    "dest": dest,
                    "part": part_number,
                    "lot": lot_number
                })
                seen.add(key)

        return destinations

    # -------------------- AUDIT --------------------

    def init_output_and_audit(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = os.path.join(
            self.destination_root,
            f"Output_{timestamp}"
        )
        os.makedirs(self.output_dir, exist_ok=True)

        self.audit_log_path = os.path.join(
            self.output_dir,
            "audit_log.csv"
        )

        with open(self.audit_log_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "Part Number",
                "Lot Number",
                "Source Path",
                "Destination Path",
                "Status",
                "Message"
            ])

    def audit(self, part, lot, source, dest, status, message=""):
        with open(self.audit_log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(timespec="seconds"),
                part,
                lot,
                source,
                dest,
                status,
                message
            ])

    def is_permission_error(self, err):
        if isinstance(err, PermissionError):
            return True
        if isinstance(err, OSError):
            if getattr(err, "errno", None) == 13:
                return True
            if getattr(err, "winerror", None) == 5:
                return True
        return False

    def log_permission_skip(self, part, lot, path, context):
        msg = f"Permission denied while {context}; skipped {path}"
        self.log.append(f"[SKIP] {msg}")
        if self.audit_log_path:
            self.audit(part, lot, path, "", "SKIPPED (NO ACCESS)", msg)

    def iter_walk_with_permissions(self, base_dir, part, lot, context):
        denied = []

        def onerror(err):
            if self.is_permission_error(err):
                path = getattr(err, "filename", base_dir)
                self.log_permission_skip(part, lot, path, context)
                denied.append(path)
            else:
                path = getattr(err, "filename", base_dir)
                msg = f"Error while {context}; {path} ({err})"
                self.log.append(f"[ERROR] {msg}")
                if self.audit_log_path:
                    self.audit(part, lot, path, "", "ERROR", msg)

        return os.walk(base_dir, onerror=onerror), denied

    # -------------------- PROCESS --------------------

    def infer_part_from_path(self, file_path):
        parts = os.path.normpath(file_path).split(os.sep)
        for idx, segment in enumerate(parts):
            if segment == BPR_FOLDER_NAME and idx > 0:
                return parts[idx - 1]

        try:
            rel = os.path.relpath(file_path, self.source_root)
            rel_parts = rel.split(os.sep)
            if len(rel_parts) >= 2:
                return rel_parts[1]
        except Exception:
            pass

        return ""

    def process_files(self):
        if not self.source_root:
            QMessageBox.warning(self, "Missing Folder",
                                "Please select a source root folder.")
            return

        self.init_output_and_audit()
        self.log.append(f"Output folder created:\n{self.output_dir}")

        valid_rows = []
        for r in range(self.bpr_table.rowCount()):
            part_item = self.bpr_table.item(r, 0)
            lot_item = self.bpr_table.item(r, 1)
            part_text = part_item.text().strip() if part_item else ""
            lot_text = lot_item.text().strip() if lot_item else ""
            if part_text or lot_text:
                valid_rows.append(r)

        self.progress.setMaximum(len(valid_rows))
        self.progress.setValue(0)

        success = 0
        failures = 0
        completed = 0
        skipped = 0

        for row in valid_rows:
            QApplication.processEvents()

            part_item = self.bpr_table.item(row, 0)
            lot_item = self.bpr_table.item(row, 1)
            part_number = part_item.text().strip() if part_item else ""
            lot_number = lot_item.text().strip() if lot_item else ""

            if not part_number and not lot_number:
                completed += 1
                self.progress.setValue(completed)
                continue

            if lot_number and not LOT_PATTERN.match(lot_number):
                msg = "Invalid lot number format"
                self.log.append(f"âœ– {lot_number}: {msg}")
                self.audit(part_number, lot_number, "", "", "INVALID LOT", msg)
                failures += 1
                completed += 1
                self.progress.setValue(completed)
                continue

            search_dir = ""
            if part_number:
                search_dir = os.path.join(
                    self.source_root,
                    part_number[:3],
                    part_number,
                    BPR_FOLDER_NAME
                )

                try:
                    os.listdir(search_dir)
                except FileNotFoundError:
                    msg = "BPR folder not found"
                    self.log.append(f"âœ– {part_number}: {msg}")
                    self.audit(part_number, lot_number, search_dir, "", "NOT FOUND", msg)
                    failures += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue
                except PermissionError:
                    msg = f"Permission denied to BPR folder; skipped {search_dir}"
                    self.log.append(f"[SKIP] {part_number}: {msg}")
                    self.audit(part_number, lot_number, search_dir, "", "SKIPPED (NO ACCESS)", msg)
                    skipped += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue
                except Exception as e:
                    msg = f"Error accessing BPR folder: {e}"
                    self.log.append(f"[ERROR] {part_number}: {msg}")
                    self.audit(part_number, lot_number, search_dir, "", "ERROR", msg)
                    failures += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue

            # ---------- LOT PROVIDED (PART PROVIDED) ----------
            if lot_number and part_number:
                matches = []
                walk_iter, denied = self.iter_walk_with_permissions(
                    search_dir,
                    part_number,
                    lot_number,
                    "searching BPR folder"
                )
                for root, _, files in walk_iter:
                    for f in files:
                        if f.lower() == f"{lot_number.lower()}.pdf":
                            matches.append(os.path.join(root, f))

                if denied:
                    skipped += len(denied)

                if len(matches) != 1:
                    msg = "PDF not found" if not matches else "Multiple matching PDFs found"
                    if denied:
                        msg = f"{msg} (access denied to {len(denied)} folder(s))"
                    self.log.append(f"âœ– {part_number}: {msg}")
                    self.audit(part_number, lot_number, search_dir, "", "LOOKUP FAILED", msg)
                    failures += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue

                source_pdf = matches[0]
                selected_lot = lot_number
                auto_selected = False

            # ---------- LOT MISSING (PART PROVIDED) ----------
            elif part_number:
                lot_candidates = {}
                walk_iter, denied = self.iter_walk_with_permissions(
                    search_dir,
                    part_number,
                    "",
                    "scanning for lot PDFs"
                )
                for root, _, files in walk_iter:
                    for f in files:
                        if not f.lower().endswith(".pdf"):
                            continue

                        lot_val = extract_6digit_lot(f)
                        if lot_val is not None:
                            lot_candidates[lot_val] = os.path.join(root, f)

                if denied:
                    skipped += len(denied)

                if not lot_candidates:
                    msg = "No PDF with valid 6-digit lot number found"
                    if denied:
                        msg = f"{msg} (access denied to {len(denied)} folder(s))"
                    self.log.append(f"âœ– {part_number}: {msg}")
                    self.audit(
                        part_number,
                        "",
                        search_dir,
                        "",
                        "LOT INFERENCE FAILED",
                        msg
                    )
                    failures += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue

                highest_lot = max(lot_candidates.keys())
                source_pdf = lot_candidates[highest_lot]
                selected_lot = str(highest_lot)
                auto_selected = True

            # ---------- PART MISSING (SEARCH ALL) ----------
            else:
                matches = []
                walk_iter, denied = self.iter_walk_with_permissions(
                    self.source_root,
                    "",
                    lot_number,
                    "searching all BPR folders"
                )
                for root, _, files in walk_iter:
                    for f in files:
                        if f.lower() == f"{lot_number.lower()}.pdf":
                            matches.append(os.path.join(root, f))

                if denied:
                    skipped += len(denied)

                if not matches:
                    msg = "PDF not found for lot number"
                    if denied:
                        msg = f"{msg} (access denied to {len(denied)} folder(s))"
                    self.log.append(f"âœ– {lot_number}: {msg}")
                    self.audit("", lot_number, self.source_root, "", "LOOKUP FAILED", msg)
                    failures += 1
                    completed += 1
                    self.progress.setValue(completed)
                    continue

                for source_pdf in matches:
                    inferred_part = self.infer_part_from_path(source_pdf)
                    if not inferred_part:
                        msg = "Unable to infer part number from path"
                        self.log.append(f"âœ– {lot_number}: {msg}")
                        self.audit("", lot_number, source_pdf, "", "PART INFERENCE FAILED", msg)
                        failures += 1
                        continue

                    dest_name = make_bpr_output_filename(inferred_part, lot_number)
                    dest_path = os.path.join(self.output_dir, dest_name)

                    try:
                        shutil.copy2(source_pdf, dest_path)
                        self.log.append(f"âœ” {inferred_part}: {dest_name}")
                        self.audit(
                            inferred_part,
                            lot_number,
                            source_pdf,
                            dest_path,
                            "COPIED",
                            ""
                        )
                        success += 1
                    except Exception as e:
                        self.log.append(f"âœ– Copy failed: {e}")
                        self.audit(
                            inferred_part,
                            lot_number,
                            source_pdf,
                            dest_path,
                            "ERROR",
                            str(e)
                        )
                        failures += 1

                completed += 1
                self.progress.setValue(completed)
                continue

            dest_name = make_bpr_output_filename(part_number, selected_lot)
            dest_path = os.path.join(self.output_dir, dest_name)

            try:
                shutil.copy2(source_pdf, dest_path)
                status = "COPIED (AUTO-SELECTED)" if auto_selected else "COPIED"
                msg = (
                    "Lot number missing â€” highest 6-digit lot inferred from filename"
                    if auto_selected else ""
                )

                self.log.append(f"âœ” {part_number}: {dest_name}")
                self.audit(
                    part_number,
                    selected_lot,
                    source_pdf,
                    dest_path,
                    status,
                    msg
                )
                success += 1

            except Exception as e:
                self.log.append(f"âœ– Copy failed: {e}")
                self.audit(
                    part_number,
                    selected_lot,
                    source_pdf,
                    dest_path,
                    "ERROR",
                    str(e)
                )
                failures += 1

            completed += 1
            self.progress.setValue(completed)

        self.log.append("\n=== SUMMARY ===")
        self.log.append(f"Successful: {success}")
        self.log.append(f"Failed: {failures}")
        self.log.append(f"Skipped: {skipped}")
        self.log.append(f"Output saved to:\n{self.output_dir}")

    # -------------------- FILE UPLOAD PROCESS --------------------

    def process_upload(self):
        files = self.get_checked_items(self.files_list)
        if not files:
            QMessageBox.warning(
                self,
                "Missing Files",
                "Please add and select at least one file."
            )
            return

        manual_destinations = [
            {"dest": path, "part": "", "lot": ""}
            for path in self.get_checked_items(self.folders_list)
        ]
        pcr_destinations = []

        if self.use_pcr_checkbox.isChecked():
            pcr_destinations = self.build_pcr_destinations()
            if not pcr_destinations:
                self.upload_log.append(
                    "No valid Part/Lot pairs found for PCR destinations."
                )

        if not manual_destinations and not pcr_destinations:
            QMessageBox.warning(
                self,
                "Missing Destinations",
                "Select folders or provide valid PCR Part/Lot pairs."
            )
            return

        destinations = []
        seen = set()
        for entry in manual_destinations + pcr_destinations:
            key = (entry["dest"], entry.get("lot", ""))
            if key in seen:
                continue
            destinations.append(entry)
            seen.add(key)

        total_ops = len(files) * len(destinations)
        self.upload_progress.setMaximum(total_ops)
        self.upload_progress.setValue(0)

        success = 0
        failures = 0
        completed = 0

        self.upload_log.append("Starting file copy...")

        for entry in destinations:
            dest = entry["dest"]
            part = entry.get("part", "")
            lot = entry.get("lot", "")
            part_lot_label = f"{part} | {lot}".strip()
            if part_lot_label == "|":
                part_lot_label = ""

            try:
                os.makedirs(dest, exist_ok=True)
            except Exception as e:
                self.upload_log.append(f"Failed to create folder: {dest} ({e})")
                failures += len(files)
                completed += len(files)
                self.upload_progress.setValue(completed)
                continue

            for file_path in files:
                QApplication.processEvents()
                original_name = os.path.basename(file_path)
                dest_name = make_dest_filename(original_name, lot)
                dest_path = os.path.join(dest, dest_name)
                try:
                    shutil.copy2(file_path, dest_path)
                    if part_lot_label:
                        self.upload_log.append(
                            f"Copied: {original_name} -> {dest_name} "
                            f"(Part|Lot: {part_lot_label}) -> {dest}"
                        )
                    else:
                        self.upload_log.append(
                            f"Copied: {original_name} -> {dest_name} -> {dest}"
                        )
                    success += 1
                except Exception as e:
                    self.upload_log.append(
                        f"Copy failed: {original_name} -> {dest_name} "
                        f"(Part|Lot: {part_lot_label}) -> {dest} ({e})"
                    )
                    failures += 1

                completed += 1
                self.upload_progress.setValue(completed)

        self.upload_log.append("\n=== SUMMARY ===")
        self.upload_log.append(f"Successful: {success}")
        self.upload_log.append(f"Failed: {failures}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PDFDownloader()
    window.show()
    sys.exit(app.exec_())
