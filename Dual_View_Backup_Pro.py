import os
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import hashlib
from datetime import datetime

class ScienceBackupApp:
    def __init__(self, root):
        self.root = root
        self.root.title("科研数据双向透视备份工具")
        self.root.geometry("1100x800")

        self.src_var = tk.StringVar()
        self.dst_var = tk.StringVar()
        self.status_var = tk.StringVar(value="等待比对...")
        
        self.setup_ui()

    def setup_ui(self):
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- 路径选择区 ---
        path_frame = ttk.LabelFrame(main_frame, text="目录配置", padding="10")
        path_frame.pack(fill=tk.X, pady=5)

        ttk.Label(path_frame, text="源文件夹 (Source):").grid(row=0, column=0, sticky="w")
        ttk.Entry(path_frame, textvariable=self.src_var, width=100).grid(row=0, column=1, padx=5)
        ttk.Button(path_frame, text="浏览", command=self.select_src).grid(row=0, column=2)

        ttk.Label(path_frame, text="目标文件夹 (Dest):").grid(row=1, column=0, sticky="w", pady=10)
        ttk.Entry(path_frame, textvariable=self.dst_var, width=100).grid(row=1, column=1, padx=5)
        ttk.Button(path_frame, text="浏览", command=self.select_dst).grid(row=1, column=2)

        # --- 控制按钮 ---
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="🔍 1. 深度扫描差异", command=self.preview).pack(side=tk.LEFT, padx=20)
        ttk.Button(btn_frame, text="🚀 2. 同步选中文件并校验MD5", command=self.run_backup).pack(side=tk.LEFT, padx=20)

        # --- 双列表显示区 ---
        paned_window = ttk.Panedwindow(main_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, pady=5)

        # 左侧：待备份
        left_frame = ttk.LabelFrame(paned_window, text="【源端】新增或已修改的文件 (待备份)", padding="5")
        self.src_list = tk.Listbox(left_frame, selectmode=tk.MULTIPLE, font=("Consolas", 9))
        self.src_list.pack(fill=tk.BOTH, expand=True)
        paned_window.add(left_frame, weight=1)

        # 右侧：冗余文件
        right_frame = ttk.LabelFrame(paned_window, text="【目标端】多出的冗余文件 (不处理)", padding="5")
        self.extra_list = tk.Listbox(right_frame, font=("Consolas", 9), fg="gray")
        self.extra_list.pack(fill=tk.BOTH, expand=True)
        paned_window.add(right_frame, weight=1)

        # 状态栏
        self.progress_label = ttk.Label(main_frame, textvariable=self.status_var, foreground="#c0392b", font=("微软雅黑", 10, "bold"))
        self.progress_label.pack(anchor="w", pady=5)

    def select_src(self):
        path = filedialog.askdirectory()
        if path: self.src_var.set(os.path.normpath(path))

    def select_dst(self):
        path = filedialog.askdirectory()
        if path: self.dst_var.set(os.path.normpath(path))

    def calculate_md5(self, file_path):
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(1024*1024), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except: return None

    def preview(self):
        src, dst = self.src_var.get(), self.dst_var.get()
        if not src or not dst:
            messagebox.showwarning("提示", "请完整选择路径")
            return

        self.src_list.delete(0, tk.END)
        self.extra_list.delete(0, tk.END)
        self.status_var.set("正在分析文件结构差异...")
        
        # /FP:全路径 /NDL:无目录 /NJH /NJS /NS /NC:精简输出
        cmd = f'robocopy "{src}" "{dst}" /E /XO /L /FP /NDL /NJH /NJS /NS /NC'

        def run_scan():
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, encoding="cp936", shell=True)
                lines = result.stdout.splitlines()
                
                to_backup = []
                extra_files = []

                for line in lines:
                    line_clean = line.strip()
                    if not line_clean: continue
                    
                    # Robocopy 标记含义：
                    # *EXTRA File -> 目标盘多出来的
                    # New File / Newer -> 需要备份的
                    
                    if "*EXTRA File" in line_clean:
                        parts = line_clean.split('\t')
                        file_path = parts[-1].strip()
                        extra_files.append(os.path.relpath(file_path, dst))
                    else:
                        # 寻找行内的路径
                        for part in line_clean.split():
                            if os.path.isabs(part) and os.path.isfile(part):
                                if part.lower().startswith(src.lower()):
                                    to_backup.append(os.path.relpath(part, src))
                                    break
                
                self.root.after(0, lambda: self.update_lists(to_backup, extra_files))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("错误", f"扫描失败: {e}"))

        threading.Thread(target=run_scan, daemon=True).start()

    def update_lists(self, to_backup, extra_files):
        for f in to_backup: self.src_list.insert(tk.END, f)
        self.src_list.select_set(0, tk.END)
        
        for f in extra_files: self.extra_list.insert(tk.END, f)
        
        self.status_var.set(f"分析完成：待备份 {len(to_backup)} 个，目标端多出 {len(extra_files)} 个。")

    def run_backup(self):
        src, dst = self.src_var.get(), self.dst_var.get()
        indices = self.src_list.curselection()
        if not indices: return

        selected = [self.src_list.get(i) for i in indices]
        
        def task():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            log_path = os.path.join(dst, f"Verification_{timestamp}.txt")
            results = []
            
            for idx, f in enumerate(selected):
                s_file = os.path.join(src, f)
                d_file = os.path.join(dst, f)
                d_dir = os.path.dirname(d_file)
                
                self.root.after(0, lambda i=idx+1: self.status_var.set(f"正在处理 ({i}/{len(selected)}): {f}"))
                
                # 1. 复制
                if not os.path.exists(d_dir): os.makedirs(d_dir)
                subprocess.run(f'robocopy "{os.path.dirname(s_file)}" "{d_dir}" "{os.path.basename(s_file)}" /XO', shell=True)
                
                # 2. 双端MD5校验
                s_md5 = self.calculate_md5(s_file)
                d_md5 = self.calculate_md5(d_file)
                match = "一致" if s_md5 == d_md5 else "错误!!"
                
                results.append(f"[{match}] {f}\n   S:{s_md5}\n   D:{d_md5}\n")

            with open(log_path, "w", encoding="utf-8") as l:
                l.write(f"备份校验日志\n源:{src}\n目标:{dst}\n" + "="*60 + "\n")
                l.writelines(results)
            
            self.root.after(0, lambda: self.finish_ui(len(selected), log_path))

        threading.Thread(target=task, daemon=True).start()

    def finish_ui(self, count, path):
        messagebox.showinfo("同步成功", f"处理完成 {count} 个文件。\n校验日志已生成至目标目录。")
        self.status_var.set("同步任务已结束")
        self.src_list.delete(0, tk.END)
        self.extra_list.delete(0, tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except: pass
    app = ScienceBackupApp(root)
    root.mainloop()