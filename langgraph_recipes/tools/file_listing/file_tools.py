from typing import Optional
from pathlib import Path
from langchain_core.tools import BaseTool
from typing import List

class ListFilesTool(BaseTool):
    name: str = "list_files"
    description: str = "Lists all files in a directory"
    
    def _run(self, directory: str = ".", pattern: Optional[str] = None) -> List[str]:
        """
        List files in the specified directory, optionally filtered by pattern.
        
        Args:
            directory (str): Path to the directory to list files from. Defaults to current directory.
            pattern (str, optional): Glob pattern to filter files (e.g., "*.py" for Python files)
        
        Returns:
            List[str]: List of file paths
        """
        path = Path(directory).resolve()
        if not path.exists():
            raise ValueError(f"Directory {directory} does not exist")
        if not path.is_dir():
            raise ValueError(f"{directory} is not a directory")
        
        if pattern:
            files = list(path.glob(pattern))
        else:
            files = list(path.glob("*"))
            
        return [str(f.relative_to(path)) for f in files if f.is_file()]