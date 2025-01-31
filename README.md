# LangGraph Recipes

A collection of examples and tools using LangGraph for building conversational AI agents.

## Tools

### Calculator

A basic arithmetic calculator tool that can perform addition, subtraction, multiplication, and division operations.

Location: `langgraph_recipes/tools/calculator.py`

Usage example:
```python
from langgraph_recipes.tools.calculator import Calculator
from langgraph_recipes.common.simple_graph_agent import Agent

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

See `langgraph_recipes/calculator_agent.py` for a complete example implementation.

## Project Structure

```
langgraph_recipes/
├── tools/              # Custom tools implementations
│   └── calculator.py   # Calculator tool
├── common/             # Shared components
│   ├── agent_state.py
│   └── simple_graph_agent.py
├── calculator_agent.py # Example calculator agent
└── tool_agent.py      # Example search agent
```

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
poetry run python langgraph_recipes/calculator_agent.py