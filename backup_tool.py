from __future__ import annotations

import csv
import hashlib
import os
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

VERIFY_QUICK = "quick"
VERIFY_SECURE = "secure"
MTIME_EPSILON_SECONDS = 1.0


@dataclass
class DiffItem:
    rel_path: str
    reason: str
    size: int
    mtime: float
    selected: bool = True
    copyable: bool = True
    category: str = "源->目标"


def format_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def format_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def format_reason(reason: str) -> str:
    mapping = {
        "NEW": "源目录新增（目标缺失）",
        "UPDATED": "源目录更新（建议复制）",
        "EXISTS_DST": "源目标均有（无需复制）",
        "EXTRA_DST": "目标目录独有（源目录没有）",
    }
    return mapping.get(reason, reason)


def parse_extension_filter(raw_text: str) -> set[str]:
    ext_set: set[str] = set()
    for token in raw_text.split(","):
        cleaned = token.strip().lower()
        if not cleaned:
            continue
        if not cleaned.startswith("."):
            cleaned = f".{cleaned}"
        ext_set.add(cleaned)
    return ext_set


def filter_candidates(
    items: list[DiffItem], ext_filter_text: str, keyword_filter_text: str
) -> list[DiffItem]:
    ext_filter = parse_extension_filter(ext_filter_text)
    keyword = keyword_filter_text.strip().lower()
    filtered: list[DiffItem] = []

    for item in items:
        suffix = Path(item.rel_path).suffix.lower()
        if ext_filter and suffix not in ext_filter:
            continue
        if keyword and keyword not in item.rel_path.lower():
            continue
        filtered.append(item)
    return filtered


def scan_incremental_candidates(src_dir: str, dst_dir: str) -> list[DiffItem]:
    src_root = Path(src_dir)
    dst_root = Path(dst_dir)
    if not src_root.is_dir():
        raise ValueError("源目录不存在或不是文件夹。")

    def build_index(root: Path) -> dict[str, os.stat_result]:
        index: dict[str, os.stat_result] = {}
        if not root.exists():
            return index
        for current_root, _, files in os.walk(root):
            root_path = Path(current_root)
            for filename in files:
                file_path = root_path / filename
                try:
                    file_stat = file_path.stat()
                except OSError:
                    continue
                rel_path = os.path.relpath(file_path, root)
                index[rel_path] = file_stat
        return index

    src_index = build_index(src_root)
    dst_index = build_index(dst_root)

    candidates: list[DiffItem] = []
    for rel_path, src_stat in src_index.items():
        dst_stat = dst_index.get(rel_path)
        if dst_stat is None:
            candidates.append(
                DiffItem(
                    rel_path=rel_path,
                    reason="NEW",
                    size=src_stat.st_size,
                    mtime=src_stat.st_mtime,
                    selected=True,
                    copyable=True,
                    category="源->目标",
                )
            )
        else:
            is_size_changed = src_stat.st_size != dst_stat.st_size
            is_newer = (src_stat.st_mtime - dst_stat.st_mtime) > MTIME_EPSILON_SECONDS
            if is_size_changed or is_newer:
                candidates.append(
                    DiffItem(
                        rel_path=rel_path,
                        reason="UPDATED",
                        size=src_stat.st_size,
                        mtime=src_stat.st_mtime,
                        selected=True,
                        copyable=True,
                        category="源->目标",
                    )
                )
            else:
                candidates.append(
                    DiffItem(
                        rel_path=rel_path,
                        reason="EXISTS_DST",
                        size=src_stat.st_size,
                        mtime=src_stat.st_mtime,
                        selected=False,
                        copyable=False,
                        category="源->目标",
                    )
                )

    for rel_path, dst_stat in dst_index.items():
        if rel_path not in src_index:
            candidates.append(
                DiffItem(
                    rel_path=rel_path,
                    reason="EXTRA_DST",
                    size=dst_stat.st_size,
                    mtime=dst_stat.st_mtime,
                    selected=False,
                    copyable=False,
                    category="目标目录独有(仅预览)",
                )
            )

    candidates.sort(key=lambda item: (item.rel_path.lower(), item.category))
    return candidates


def calculate_md5(file_path: Path) -> str | None:
    hasher = hashlib.md5()
    try:
        with file_path.open("rb") as file_obj:
            for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
                hasher.update(chunk)
    except OSError:
        return None
    return hasher.hexdigest()


def write_verification_log(
    src_dir: str,
    dst_dir: str,
    results: list[dict[str, str]],
    total: int,
    success_count: int,
    failed_count: int,
) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = Path(dst_dir) / f"Verification_{timestamp}.csv"

    header = [
        "记录类型",
        "运行时间",
        "源目录",
        "目标目录",
        "总任务数",
        "成功数",
        "失败数",
        "序号",
        "状态",
        "相对路径",
        "源文件MD5(SRC_MD5)",
        "目标文件MD5(DST_MD5)",
        "错误信息",
    ]

    with log_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(header)
        writer.writerow(
            [
                "SUMMARY",
                run_time,
                src_dir,
                dst_dir,
                total,
                success_count,
                failed_count,
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )

        for index, item in enumerate(results, start=1):
            writer.writerow(
                [
                    "DETAIL",
                    run_time,
                    src_dir,
                    dst_dir,
                    total,
                    success_count,
                    failed_count,
                    index,
                    item.get("status", ""),
                    item.get("rel_path", ""),
                    item.get("src_md5", "") or "",
                    item.get("dst_md5", "") or "",
                    item.get("error", "") or "",
                ]
            )

    return str(log_path)


def run_incremental_backup(
    src_dir: str,
    dst_dir: str,
    items: Iterable[DiffItem],
    verify_mode: str = VERIFY_QUICK,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
) -> dict[str, object]:
    selected_items = [item for item in items if item.selected and item.copyable]
    total = len(selected_items)
    success_count = 0
    failed_count = 0
    failed_files: list[str] = []
    results: list[dict[str, str]] = []

    for index, item in enumerate(selected_items, start=1):
        src_file = Path(src_dir) / item.rel_path
        dst_file = Path(dst_dir) / item.rel_path

        status = "OK"
        error_msg = ""
        src_md5 = ""
        dst_md5 = ""

        try:
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst_file)

            if verify_mode == VERIFY_SECURE:
                src_md5 = calculate_md5(src_file) or ""
                dst_md5 = calculate_md5(dst_file) or ""
                if not src_md5 or not dst_md5:
                    status = "FAIL"
                    error_msg = "MD5 计算失败"
                elif src_md5 != dst_md5:
                    status = "FAIL"
                    error_msg = "MD5 不一致"
        except Exception as exc:  # pylint: disable=broad-except
            status = "FAIL"
            error_msg = str(exc)

        if status == "OK":
            success_count += 1
        else:
            failed_count += 1
            failed_files.append(item.rel_path)

        if verify_mode == VERIFY_SECURE:
            results.append(
                {
                    "status": status,
                    "rel_path": item.rel_path,
                    "src_md5": src_md5,
                    "dst_md5": dst_md5,
                    "error": error_msg,
                }
            )

        if progress_callback:
            progress_callback(index, total, item.rel_path, status)

    log_path = ""
    if verify_mode == VERIFY_SECURE:
        log_path = write_verification_log(
            src_dir=src_dir,
            dst_dir=dst_dir,
            results=results,
            total=total,
            success_count=success_count,
            failed_count=failed_count,
        )

    return {
        "total": total,
        "success_count": success_count,
        "failed_count": failed_count,
        "failed_files": failed_files,
        "log_path": log_path,
    }


class ScienceBackupApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("预览增量备份助手")
        self.root.geometry("1700x980")
        self.root.minsize(1560, 900)

        self.src_var = tk.StringVar()
        self.dst_var = tk.StringVar()
        self.ext_filter_var = tk.StringVar()
        self.keyword_filter_var = tk.StringVar()
        self.verify_mode_var = tk.StringVar(value=VERIFY_QUICK)
        self.status_var = tk.StringVar(value="就绪")
        self.counter_var = tk.StringVar(
            value="差异 0 | 可复制 0 | 左列只读 0 | 右列独有 0 | 左侧显示 0 | 右侧显示 0 | 已勾选复制 0（左侧显示中 0）"
        )

        self.all_items: list[DiffItem] = []
        self.filtered_copyable_items: list[DiffItem] = []
        self.filtered_preview_only_items: list[DiffItem] = []
        self.copy_row_item_map: dict[str, DiffItem] = {}
        self.copy_sort_col = "path"
        self.copy_sort_desc = False
        self.preview_sort_col = "path"
        self.preview_sort_desc = False

        self.base_font = ("Microsoft YaHei UI", 14)
        self.tree_font = ("Consolas", 13)
        self.tree_header_font = ("Microsoft YaHei UI", 14, "bold")
        self.setup_ui()

    def setup_ui(self) -> None:
        style = ttk.Style(self.root)
        style.configure(".", font=self.base_font)
        style.configure("Treeview", font=self.tree_font, rowheight=34)
        style.configure("Treeview.Heading", font=self.tree_header_font)
        self.root.option_add("*Font", self.base_font)

        main_frame = ttk.Frame(self.root, padding=12)
        main_frame.pack(fill=tk.BOTH, expand=True)

        path_frame = ttk.LabelFrame(main_frame, text="目录设置", padding=10)
        path_frame.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(path_frame, text="源目录:").grid(row=0, column=0, sticky="w")
        ttk.Entry(path_frame, textvariable=self.src_var, width=120).grid(row=0, column=1, padx=6, sticky="we")
        ttk.Button(path_frame, text="浏览", command=self.select_src).grid(row=0, column=2, padx=(0, 4))

        ttk.Label(path_frame, text="目标目录:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(path_frame, textvariable=self.dst_var, width=120).grid(row=1, column=1, padx=6, pady=(8, 0), sticky="we")
        ttk.Button(path_frame, text="浏览", command=self.select_dst).grid(row=1, column=2, padx=(0, 4), pady=(8, 0))
        path_frame.columnconfigure(1, weight=1)

        rule_frame = ttk.LabelFrame(main_frame, text="自定义过滤与校验模式", padding=10)
        rule_frame.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(rule_frame, text="扩展名过滤(逗号分隔):").grid(row=0, column=0, sticky="w")
        ttk.Entry(rule_frame, textvariable=self.ext_filter_var, width=44).grid(row=0, column=1, padx=(6, 10), sticky="w")
        ttk.Label(rule_frame, text="关键词过滤:").grid(row=0, column=2, sticky="w")
        ttk.Entry(rule_frame, textvariable=self.keyword_filter_var, width=36).grid(row=0, column=3, padx=(6, 10), sticky="w")

        self.apply_filter_btn = ttk.Button(rule_frame, text="应用过滤并全选结果", command=self.apply_filters)
        self.apply_filter_btn.grid(row=0, column=4, padx=(0, 6))
        self.clear_filter_btn = ttk.Button(rule_frame, text="清空过滤", command=self.clear_filters)
        self.clear_filter_btn.grid(row=0, column=5)

        ttk.Label(rule_frame, text="校验模式:").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Radiobutton(
            rule_frame,
            text="快速模式（仅复制）",
            variable=self.verify_mode_var,
            value=VERIFY_QUICK,
        ).grid(row=1, column=1, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Radiobutton(
            rule_frame,
            text="安全模式（MD5+日志）",
            variable=self.verify_mode_var,
            value=VERIFY_SECURE,
        ).grid(row=1, column=3, columnspan=2, sticky="w", pady=(10, 0))

        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=(0, 8))

        self.scan_btn = ttk.Button(action_frame, text="1. 扫描预览", command=self.preview)
        self.scan_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.backup_btn = ttk.Button(action_frame, text="2. 执行增量备份", command=self.run_backup)
        self.backup_btn.pack(side=tk.LEFT, padx=(0, 20))

        self.select_visible_btn = ttk.Button(
            action_frame, text="勾选左列当前显示项", command=lambda: self.set_visible_selected(True)
        )
        self.select_visible_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.unselect_visible_btn = ttk.Button(
            action_frame, text="取消左列当前显示项", command=lambda: self.set_visible_selected(False)
        )
        self.unselect_visible_btn.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Label(
            main_frame,
            text="提示：点击任意列表的列名可排序，再次点击可切换升序/降序。",
            foreground="#666666",
        ).pack(anchor="w", pady=(0, 6))

        preview_container = ttk.Panedwindow(main_frame, orient=tk.HORIZONTAL)
        preview_container.pack(fill=tk.BOTH, expand=True)

        left_frame = ttk.LabelFrame(preview_container, text="左列：源->目标（可复制，支持勾选）", padding=8)
        right_frame = ttk.LabelFrame(preview_container, text="右列：目标目录独有文件（源目录没有，仅查看）", padding=8)
        preview_container.add(left_frame, weight=4)
        preview_container.add(right_frame, weight=3)

        left_columns = ("selected", "reason", "size", "mtime", "path")
        self.copy_tree = ttk.Treeview(
            left_frame,
            columns=left_columns,
            show="headings",
            selectmode="extended",
        )
        self.copy_tree.column("selected", width=68, anchor="center", stretch=False)
        self.copy_tree.column("reason", width=210, anchor="center", stretch=False)
        self.copy_tree.column("size", width=120, anchor="e", stretch=False)
        self.copy_tree.column("mtime", width=180, anchor="center", stretch=False)
        self.copy_tree.column("path", width=880, anchor="w")

        left_y_scroll = ttk.Scrollbar(left_frame, orient=tk.VERTICAL, command=self.copy_tree.yview)
        left_x_scroll = ttk.Scrollbar(left_frame, orient=tk.HORIZONTAL, command=self.copy_tree.xview)
        self.copy_tree.configure(yscrollcommand=left_y_scroll.set, xscrollcommand=left_x_scroll.set)
        self.copy_tree.grid(row=0, column=0, sticky="nsew")
        left_y_scroll.grid(row=0, column=1, sticky="ns")
        left_x_scroll.grid(row=1, column=0, sticky="ew")
        left_frame.rowconfigure(0, weight=1)
        left_frame.columnconfigure(0, weight=1)

        right_columns = ("reason", "size", "mtime", "path")
        self.preview_tree = ttk.Treeview(
            right_frame,
            columns=right_columns,
            show="headings",
            selectmode="browse",
        )
        self.preview_tree.column("reason", width=220, anchor="center", stretch=False)
        self.preview_tree.column("size", width=120, anchor="e", stretch=False)
        self.preview_tree.column("mtime", width=180, anchor="center", stretch=False)
        self.preview_tree.column("path", width=640, anchor="w")

        right_y_scroll = ttk.Scrollbar(right_frame, orient=tk.VERTICAL, command=self.preview_tree.yview)
        right_x_scroll = ttk.Scrollbar(right_frame, orient=tk.HORIZONTAL, command=self.preview_tree.xview)
        self.preview_tree.configure(yscrollcommand=right_y_scroll.set, xscrollcommand=right_x_scroll.set)
        self.preview_tree.grid(row=0, column=0, sticky="nsew")
        right_y_scroll.grid(row=0, column=1, sticky="ns")
        right_x_scroll.grid(row=1, column=0, sticky="ew")
        right_frame.rowconfigure(0, weight=1)
        right_frame.columnconfigure(0, weight=1)

        self.copy_tree.bind("<Button-1>", self.on_copy_tree_click, add="+")
        self.update_sort_headers()

        ttk.Label(main_frame, textvariable=self.counter_var, foreground="#0b5cab").pack(anchor="w", pady=(8, 2))
        ttk.Label(main_frame, textvariable=self.status_var, foreground="#333333").pack(anchor="w")

    def select_src(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.src_var.set(os.path.normpath(path))

    def select_dst(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.dst_var.set(os.path.normpath(path))

    def set_busy(self, busy: bool) -> None:
        state = tk.DISABLED if busy else tk.NORMAL
        self.scan_btn.config(state=state)
        self.backup_btn.config(state=state)
        self.apply_filter_btn.config(state=state)
        self.clear_filter_btn.config(state=state)
        self.select_visible_btn.config(state=state)
        self.unselect_visible_btn.config(state=state)

    def preview(self) -> None:
        src = self.src_var.get().strip()
        dst = self.dst_var.get().strip()

        if not src or not dst:
            messagebox.showwarning("提示", "请先选择源目录和目标目录。")
            return
        if not os.path.isdir(src):
            messagebox.showwarning("提示", "源目录不存在，请重新选择。")
            return

        self.set_busy(True)
        self.status_var.set("正在扫描差异文件，请稍候...")
        self.copy_tree.delete(*self.copy_tree.get_children())
        self.preview_tree.delete(*self.preview_tree.get_children())
        self.counter_var.set("差异 0 | 可复制 0 | 左列只读 0 | 右列独有 0 | 左侧显示 0 | 右侧显示 0 | 已勾选复制 0（左侧显示中 0）")

        def worker() -> None:
            try:
                items = scan_incremental_candidates(src, dst)
            except Exception as exc:  # pylint: disable=broad-except
                self.root.after(0, lambda: self.on_scan_failed(str(exc)))
                return
            self.root.after(0, lambda: self.on_scan_done(items))

        threading.Thread(target=worker, daemon=True).start()

    def on_scan_done(self, items: list[DiffItem]) -> None:
        self.all_items = items
        self.apply_filters(select_filtered_only=True)
        self.set_busy(False)

        if not items:
            self.status_var.set("扫描完成：两个目录无差异。")
            messagebox.showinfo("提示", "没有发现两个目录之间的差异文件。")
        else:
            copyable_count = sum(1 for item in items if item.copyable)
            left_readonly_count = sum(
                1 for item in items if item.category == "源->目标" and not item.copyable
            )
            right_preview_count = sum(1 for item in items if item.category != "源->目标")
            self.status_var.set(
                f"扫描完成：可复制 {copyable_count} 个，左列只读 {left_readonly_count} 个，右列独有文件 {right_preview_count} 个。"
            )

    def on_scan_failed(self, error_msg: str) -> None:
        self.set_busy(False)
        self.status_var.set("扫描失败。")
        messagebox.showerror("错误", f"扫描失败：{error_msg}")

    def apply_filters(self, select_filtered_only: bool = True) -> None:
        filtered_all = filter_candidates(
            self.all_items,
            self.ext_filter_var.get(),
            self.keyword_filter_var.get(),
        )
        self.filtered_copyable_items = [item for item in filtered_all if item.category == "源->目标"]
        self.filtered_preview_only_items = [item for item in filtered_all if item.category != "源->目标"]

        if select_filtered_only:
            visible_ids = {id(item) for item in self.filtered_copyable_items if item.copyable}
            for item in self.all_items:
                if item.copyable:
                    item.selected = id(item) in visible_ids

        self.refresh_trees()
        self.refresh_counter()

    def clear_filters(self) -> None:
        self.ext_filter_var.set("")
        self.keyword_filter_var.set("")
        self.apply_filters(select_filtered_only=True)

    def update_sort_headers(self) -> None:
        copy_labels = {
            "selected": "复制",
            "reason": "状态",
            "size": "大小",
            "mtime": "修改时间",
            "path": "相对路径",
        }
        for col, label in copy_labels.items():
            arrow = ""
            if self.copy_sort_col == col:
                arrow = " ↓" if self.copy_sort_desc else " ↑"
            self.copy_tree.heading(
                col,
                text=f"{label}{arrow}",
                command=lambda c=col: self.toggle_copy_sort(c),
            )

        preview_labels = {
            "reason": "状态",
            "size": "大小",
            "mtime": "修改时间",
            "path": "相对路径",
        }
        for col, label in preview_labels.items():
            arrow = ""
            if self.preview_sort_col == col:
                arrow = " ↓" if self.preview_sort_desc else " ↑"
            self.preview_tree.heading(
                col,
                text=f"{label}{arrow}",
                command=lambda c=col: self.toggle_preview_sort(c),
            )

    def toggle_copy_sort(self, column: str) -> None:
        if self.copy_sort_col == column:
            self.copy_sort_desc = not self.copy_sort_desc
        else:
            self.copy_sort_col = column
            self.copy_sort_desc = False
        self.refresh_trees()

    def toggle_preview_sort(self, column: str) -> None:
        if self.preview_sort_col == column:
            self.preview_sort_desc = not self.preview_sort_desc
        else:
            self.preview_sort_col = column
            self.preview_sort_desc = False
        self.refresh_trees()

    def copy_sort_key(self, item: DiffItem):
        if self.copy_sort_col == "selected":
            if not item.copyable:
                return -1
            return 1 if item.selected else 0
        if self.copy_sort_col == "reason":
            order = {"UPDATED": 0, "NEW": 1, "EXISTS_DST": 2}
            return order.get(item.reason, 99)
        if self.copy_sort_col == "size":
            return item.size
        if self.copy_sort_col == "mtime":
            return item.mtime
        return item.rel_path.lower()

    def preview_sort_key(self, item: DiffItem):
        if self.preview_sort_col == "reason":
            order = {"EXTRA_DST": 0}
            return order.get(item.reason, 99)
        if self.preview_sort_col == "size":
            return item.size
        if self.preview_sort_col == "mtime":
            return item.mtime
        return item.rel_path.lower()

    def refresh_trees(self) -> None:
        self.copy_tree.delete(*self.copy_tree.get_children())
        self.preview_tree.delete(*self.preview_tree.get_children())
        self.copy_row_item_map.clear()

        sorted_copy_items = sorted(
            self.filtered_copyable_items,
            key=self.copy_sort_key,
            reverse=self.copy_sort_desc,
        )
        sorted_preview_items = sorted(
            self.filtered_preview_only_items,
            key=self.preview_sort_key,
            reverse=self.preview_sort_desc,
        )

        for idx, item in enumerate(sorted_copy_items):
            row_id = f"copy_row_{idx}"
            mark = "只读" if not item.copyable else ("✓" if item.selected else "")
            self.copy_tree.insert(
                "",
                tk.END,
                iid=row_id,
                values=(
                    mark,
                    format_reason(item.reason),
                    format_size(item.size),
                    format_mtime(item.mtime),
                    item.rel_path,
                ),
            )
            self.copy_row_item_map[row_id] = item

        for idx, item in enumerate(sorted_preview_items):
            row_id = f"preview_row_{idx}"
            self.preview_tree.insert(
                "",
                tk.END,
                iid=row_id,
                values=(
                    format_reason(item.reason),
                    format_size(item.size),
                    format_mtime(item.mtime),
                    item.rel_path,
                ),
            )
        self.update_sort_headers()

    def refresh_counter(self) -> None:
        total = len(self.all_items)
        copyable_total = sum(1 for item in self.all_items if item.copyable)
        left_readonly_total = sum(
            1 for item in self.all_items if item.category == "源->目标" and not item.copyable
        )
        right_preview_total = sum(1 for item in self.all_items if item.category != "源->目标")
        selected_total = sum(1 for item in self.all_items if item.selected and item.copyable)
        left_visible = len(self.filtered_copyable_items)
        right_visible = len(self.filtered_preview_only_items)
        selected_left_visible = sum(
            1 for item in self.filtered_copyable_items if item.copyable and item.selected
        )
        self.counter_var.set(
            "差异 "
            f"{total} | 可复制 {copyable_total} | 左列只读 {left_readonly_total} | "
            f"右列独有 {right_preview_total} | "
            f"左侧显示 {left_visible} | 右侧显示 {right_visible} | "
            f"已勾选复制 {selected_total}（左侧显示中 {selected_left_visible}）"
        )

    def on_copy_tree_click(self, event: tk.Event) -> str | None:
        column = self.copy_tree.identify_column(event.x)
        row_id = self.copy_tree.identify_row(event.y)
        if column != "#1" or not row_id:
            return None

        item = self.copy_row_item_map.get(row_id)
        if not item:
            return None

        if not item.copyable:
            return "break"

        item.selected = not item.selected
        new_mark = "✓" if item.selected else ""
        values = list(self.copy_tree.item(row_id, "values"))
        values[0] = new_mark
        self.copy_tree.item(row_id, values=values)
        self.refresh_counter()
        return "break"

    def set_visible_selected(self, selected: bool) -> None:
        if not self.filtered_copyable_items:
            return
        for item in self.filtered_copyable_items:
            if item.copyable:
                item.selected = selected
        self.refresh_trees()
        self.refresh_counter()

    def run_backup(self) -> None:
        src = self.src_var.get().strip()
        dst = self.dst_var.get().strip()
        selected_items = [item for item in self.all_items if item.selected and item.copyable]
        mode = self.verify_mode_var.get()

        if not src or not dst:
            messagebox.showwarning("提示", "请先选择源目录和目标目录。")
            return
        if not selected_items:
            messagebox.showwarning("提示", "当前没有可复制且已勾选的文件。")
            return

        try:
            Path(dst).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            messagebox.showerror("错误", f"无法创建目标目录：{exc}")
            return

        self.set_busy(True)
        self.status_var.set("正在执行增量备份，请勿关闭程序...")

        def progress(done: int, total: int, rel_path: str, status: str) -> None:
            self.root.after(
                0,
                lambda: self.status_var.set(
                    f"备份中 {done}/{total}: [{status}] {rel_path}"
                ),
            )

        def worker() -> None:
            try:
                result = run_incremental_backup(
                    src_dir=src,
                    dst_dir=dst,
                    items=selected_items,
                    verify_mode=mode,
                    progress_callback=progress,
                )
            except Exception as exc:  # pylint: disable=broad-except
                self.root.after(0, lambda: self.on_backup_failed(str(exc)))
                return
            self.root.after(0, lambda: self.on_backup_done(result, mode))

        threading.Thread(target=worker, daemon=True).start()

    def on_backup_done(self, result: dict[str, object], mode: str) -> None:
        self.set_busy(False)

        total = int(result["total"])
        success_count = int(result["success_count"])
        failed_count = int(result["failed_count"])
        failed_files = list(result["failed_files"])
        log_path = str(result["log_path"])

        summary_lines = [
            f"总任务数: {total}",
            f"成功: {success_count}",
            f"失败: {failed_count}",
        ]

        if mode == VERIFY_SECURE and log_path:
            summary_lines.append(f"校验CSV日志: {log_path}")
        if failed_files:
            preview = "\n".join(failed_files[:8])
            if len(failed_files) > 8:
                preview += "\n..."
            summary_lines.append(f"失败文件:\n{preview}")

        self.status_var.set(
            f"备份完成：成功 {success_count}，失败 {failed_count}。"
        )
        messagebox.showinfo("完成", "\n".join(summary_lines))

    def on_backup_failed(self, error_msg: str) -> None:
        self.set_busy(False)
        self.status_var.set("备份失败。")
        messagebox.showerror("错误", f"备份失败：{error_msg}")


if __name__ == "__main__":
    root = tk.Tk()
    try:
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    app = ScienceBackupApp(root)
    root.mainloop()
