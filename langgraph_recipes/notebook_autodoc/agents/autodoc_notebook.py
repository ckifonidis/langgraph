from pprint import pprint
import os
from typing import Union, Dict, List

from langchain.prompts import ChatPromptTemplate
from dotenv import load_dotenv

from utils.model_selector import use_model, ModelType, OpenAIModels

load_dotenv()

# Define path constants
BASE_PATH = 'databricks_notebook/merchant_insights/api'
SYSTEM_PROMPT_PATH = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/prompts/api_func_design_prompt.md')
NOTEBOOKS_DIR = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/notebooks/merchant_promotion_insights')
TABLE_DESCRIPTIONS_DIR = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/table_descriptions')

def process_notebook(notebook_path: str) -> None:
    """Process a single notebook and generate its documentation."""
    output_path = os.path.join(os.path.dirname(notebook_path), 
                              os.path.splitext(os.path.basename(notebook_path))[0] + '.md')
    
    # Load the system prompt
    with open(SYSTEM_PROMPT_PATH, 'r') as file:
        system_prompt = file.read()
    
    # Load the notebook content
    with open(notebook_path, 'r') as file:
        notebook_content = file.read()
    
    # Load and combine table descriptions
    table_descriptions = ""
    for filename in os.listdir(TABLE_DESCRIPTIONS_DIR):
        if filename.endswith('.sql'):
            file_path = os.path.join(TABLE_DESCRIPTIONS_DIR, filename)
            with open(file_path, 'r') as file:
                table_descriptions += f"\n-- {filename}\n" + file.read()
    
    # Initialize model
    model = use_model(
        ModelType.OPENAI,
        model_name=OpenAIModels.GPT_4O_MINI
    )
    
    # Configuration for the prompt template
    template_vars = {
        "notebook_content": notebook_content,
        "source_table_descriptions": table_descriptions
    }
    
    messages_template = [
        ("system", system_prompt),
        ("human", "Please analyze the notebook and create a functional specification document."),
    ]
    prompt_template = ChatPromptTemplate.from_messages(messages_template)
    messages = prompt_template.invoke(template_vars)
    result = model.invoke(messages)
    
    # Handle the result content safely
    content = str(result.content) if hasattr(result, 'content') else str(result)
    
    # Remove markdown code block if present
    if isinstance(content, str):
        if content.startswith('```'):
            content = content.split('\n', 1)[1]  # Remove first line
        if content.endswith('```'):
            content = content.rsplit('\n', 1)[0]  # Remove last line
    
    print(f"Saving markdown to: {output_path}")
    with open(output_path, 'w') as file:
        file.write(content)
    pprint(content)

def main(single_file: str = None) -> None:
    """Process Python notebooks in the merchant promotion insights directory.
    
    Args:
        single_file (str, optional): Path to a single Python file to process.
            If not provided, processes all Python files in NOTEBOOKS_DIR.
    """
    if single_file:
        if not os.path.exists(single_file):
            raise FileNotFoundError(f"File not found: {single_file}")
        if not single_file.endswith('.py'):
            raise ValueError("Only Python files (.py) are supported")
        print(f"\nProcessing single notebook: {os.path.basename(single_file)}")
        process_notebook(single_file)
    else:
        # Process all Python files in the notebooks directory
        for filename in os.listdir(NOTEBOOKS_DIR):
            if filename.endswith('.py'):
                notebook_path = os.path.join(NOTEBOOKS_DIR, filename)
                print(f"\nProcessing notebook: {filename}")
                process_notebook(notebook_path)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
