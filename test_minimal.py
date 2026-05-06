import sys
import os

log_path = os.path.join(os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else '.', 'minimal_log.txt')
with open(log_path, 'w') as f:
    f.write(f"Python {sys.version}\n")
    f.write(f"Frozen: {getattr(sys, 'frozen', False)}\n")
    f.write(f"Exe: {sys.executable}\n")
    f.write("SUCCESS\n")

print("Done! Check minimal_log.txt")
input("Press Enter...")
