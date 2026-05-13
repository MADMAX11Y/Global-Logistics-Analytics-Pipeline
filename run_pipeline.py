"""
Global Logistics ETL Pipeline Runner
Run: python run_pipeline.py
"""

import subprocess
import sys
import os
from pathlib import Path

def run_pipeline():
    """Execute the complete ETL pipeline."""
    
    print("\n" + "="*70)
    print("🚀 GLOBAL LOGISTICS ETL PIPELINE")
    print("="*70)
    
    # Check if virtual environment is active
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("⚠️  Warning: Virtual environment not detected!")
        print("   Consider activating: .\\.venv-1\\Scripts\\Activate.ps1")
    
    # Check if orchestration script exists
    orch_script = Path("notebooks") / "06_orchestration.py"
    if not orch_script.exists():
        print(f"❌ Orchestration script not found: {orch_script}")
        return False
    
    # Check if database exists (optional)
    db_path = Path("data") / "pipeline_analytics.duckdb"
    if db_path.exists():
        print(f"📁 Existing database found: {db_path}")
    else:
        print("📁 No existing database - will create new one")
    
    print("\n" + "-"*70)
    print("EXECUTING PIPELINE...")
    print("-"*70)
    
    try:
        # Run the orchestration script
        result = subprocess.run([
            sys.executable, 
            str(orch_script)
        ], capture_output=True, text=True, cwd=Path.cwd())
        
        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print("\n" + "="*70)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*70)
            print("\n📊 Next steps:")
            print("   • Check database: python check_database.py")
            print("   • Run dashboard: streamlit run notebooks/05_dashboard.py")
            return True
        else:
            print(f"\n❌ PIPELINE FAILED with exit code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"\n❌ ERROR running pipeline: {e}")
        return False

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)