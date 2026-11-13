# Test the working tree (src/PanDAirflow), not an older PanDAirflow installed in the environment.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
