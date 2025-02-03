from pprint import pprint
import os

from langchain.prompts import ChatPromptTemplate
from dotenv import load_dotenv

from utils.model_selector import use_model, ModelType, OpenAIModels

load_dotenv()

# Define path constants
BASE_PATH = 'databricks_notebook/merchant_insights/api'
SYSTEM_PROMPT_PATH = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/prompts/api_func_design_prompt.md')
NOTEBOOK_PATH = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/notebooks/merchant_promotion_insights/AnalyticalCardsCustomerCharacteristics.py')
TABLE_DESCRIPTIONS_DIR = os.path.join('/home/ckifonidis/ai/langgraph/langgraph_recipes/notebook_autodoc/input/table_descriptions')
OUTPUT_PATH = os.path.join(os.path.dirname(NOTEBOOK_PATH), os.path.splitext(os.path.basename(NOTEBOOK_PATH))[0] + '.md')

# Load the system prompt from file
with open(SYSTEM_PROMPT_PATH, 'r') as file:
    system_prompt = file.read()

# Load the notebook content
with open(NOTEBOOK_PATH, 'r') as file:
    notebook_content = file.read()

# Load and combine table descriptions from directory
table_descriptions = ""

# Read all SQL files from the directory
for filename in os.listdir(TABLE_DESCRIPTIONS_DIR):
    if filename.endswith('.sql'):
        file_path = os.path.join(TABLE_DESCRIPTIONS_DIR, filename)
        with open(file_path, 'r') as file:
            table_descriptions += f"\n-- {filename}\n" + file.read()

# Initialize model using model selector
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

# Save the markdown output
content = result.content
# Remove markdown code block if present
if content.startswith('```'):
    content = content.split('\n', 1)[1]  # Remove first line
if content.endswith('```'):
    content = content.rsplit('\n', 1)[0]  # Remove last line

print(f"Saving markdown to: {OUTPUT_PATH}")
with open(OUTPUT_PATH, 'w') as file:
    file.write(content)
pprint(result.content)
