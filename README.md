# LangGraph Recipes

A collection of examples and tools using LangGraph for building conversational AI agents and automated documentation workflows.

## Features

### Notebook Auto-Documentation
Located in `langgraph_recipes/notebook_autodoc/`, this feature provides automated documentation generation for Databricks notebooks:
- Processes input Databricks notebooks from specified directories
- Combines documentation using configurable prompts
- Supports SQL table descriptions integration
- Includes specialized agents for documentation and markdown combining

### Tools

#### Calculator
A basic arithmetic calculator tool for performing mathematical operations.

Location: `langgraph_recipes/tools/calculator/calculator.py`

Usage example:
```python
from langgraph_recipes.tools.calculator import Calculator
from langgraph_recipes.common.agents.agent_simple import Agent

# Initialize the calculator tool
calculator = Calculator()

# Create an agent with the calculator tool
agent = Agent(model, [calculator], system="Your system prompt here")

# Use the agent
result = agent.graph.invoke({
    'messages': [HumanMessage(content="What is 25 + 15?")]
})
```

Input schema:
- `operation`: String (+, -, *, /)
- `a`: First number
- `b`: Second number

See `langgraph_recipes/recipe/calculator_agent.py` for a complete example implementation.

#### File Listing
Tools for file system operations and listings, located in `langgraph_recipes/tools/file_listing/`.

## Project Structure

```
langgraph_recipes/
├── common/                 # Shared components
│   ├── agent_state.py     # Agent state management
│   └── agents/            # Base agent implementations
├── notebook_autodoc/      # Notebook documentation tools
│   ├── agents/           # Documentation specific agents
│   ├── input/            # Input resources
│   │   ├── notebooks/    # Source Databricks notebooks
│   │   ├── prompts/      # Documentation prompts
│   │   └── table_descriptions/ # SQL schema descriptions
├── recipe/               # Example agent implementations
│   ├── calculator_agent.py
│   ├── chat_agent.py
│   └── tool_agent.py
├── tools/                # Tool implementations
│   ├── calculator/      # Calculator tool
│   └── file_listing/    # File system tools
└── __init__.py
```

## Requirements

- Python >= 3.11
- Dependencies:
  - langgraph >= 0.2.68
  - pydantic >= 2.10.6
  - langchain-openai >= 0.3.2
  - langchain-anthropic >= 0.3.4
  - langchain-community >= 0.3.16
  - python-dotenv >= 1.0.1

## Getting Started

1. Install dependencies:
```bash
poetry install
```

2. Set up your environment variables in `.env`:
```
OPENAI_API_KEY=your_key_here
```

3. Run an example:
```bash
poetry run python langgraph_recipes/recipe/calculator_agent.py
```

## Examples

The project includes several example implementations:
- Calculator agent (`recipe/calculator_agent.py`)
- Chat agent (`recipe/chat_agent.py`)
- Tool-using agent (`recipe/tool_agent.py`)
- Databricks notebook documentation workflow (`notebook_autodoc/`)