from pprint import pprint
import os
from typing import List
import glob

from langchain.prompts import ChatPromptTemplate
from dotenv import load_dotenv

from utils.model_selector import use_model, ModelType, OpenAIModels

load_dotenv()

# Define path constants
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_DIR = os.path.join(ROOT_DIR, '__output__')

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_markdown_files(directory: str) -> List[dict]:
    """Read all markdown files from the specified directory.
    
    Args:
        directory (str): Path to the directory containing markdown files.
    
    Returns:
        List[dict]: List of dictionaries containing filename and content.
    """
    markdown_files = []
    for filepath in glob.glob(os.path.join(directory, "*.md")):
        with open(filepath, 'r') as file:
            markdown_files.append({
                'filename': os.path.basename(filepath),
                'content': file.read()
            })
    return markdown_files

def combine_markdown_files(input_dir: str, output_filename: str = None) -> None:
    """Combine multiple markdown files into a single documentation file.
    
    Args:
        input_dir (str): Path to the directory containing markdown files.
        output_filename (str, optional): Name of the output file. 
            If not provided, defaults to "combined_documentation.md".
    """
    if output_filename is None:
        output_filename = "combined_documentation.md"
        
    # Load the system prompt
    prompt_path = os.path.join(ROOT_DIR, 'notebook_autodoc', 'input', 'prompts', 'markdown_combiner_prompt.md')
    with open(prompt_path, 'r') as file:
        system_prompt = file.read()
    
    # Read all markdown files
    markdown_files = read_markdown_files(input_dir)
    
    if not markdown_files:
        print(f"No markdown files found in directory: {input_dir}")
        return
    
    # Initialize model
    model = use_model(
        ModelType.OPENAI,
        model_name=OpenAIModels.GPT_4O_MINI
    )
    
    # Prepare the content for the prompt
    files_content = "\n\n=== FILE SEPARATOR ===\n\n".join(
        f"File: {file['filename']}\n\n{file['content']}"
        for file in markdown_files
    )
    
    # Configuration for the prompt template
    template_vars = {
        "files": files_content,
        "file_count": len(markdown_files),
        "directory": os.path.basename(input_dir)
    }
    
    messages_template = [
        ("system", system_prompt),
        ("human", "Please combine these markdown files into a single cohesive document:\n\n{files}"),
    ]
    prompt_template = ChatPromptTemplate.from_messages(messages_template)
    messages = prompt_template.invoke(template_vars)
    result = model.invoke(messages)
    
    # Handle the result content safely
    content = str(result.content) if hasattr(result, 'content') else str(result)
    
    # Remove markdown code block if present
    if content.startswith('```'):
        content = content.split('\n', 1)[1]
    if content.endswith('```'):
        content = content.rsplit('\n', 1)[0]
    
    # Save to output directory, maintaining a flat structure
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    print(f"Saving combined markdown to: {output_path}")
    with open(output_path, 'w') as file:
        file.write(content)
    pprint(content)

def main(input_dir: str = None, output_file: str = None) -> None:
    """Process markdown files in the specified directory.
    
    Args:
        input_dir (str, optional): Path to directory containing markdown files.
            If not provided, will use the current directory.
        output_file (str, optional): Name of the output file.
            If not provided, defaults to "combined_documentation.md".
    """
    if input_dir is None:
        input_dir = os.getcwd()
    
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Directory not found: {input_dir}")
    
    print(f"\nProcessing markdown files in: {input_dir}")
    combine_markdown_files(input_dir, output_file)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        main(input_dir, output_file)
    else:
        main()