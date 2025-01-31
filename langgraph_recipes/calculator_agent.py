from langchain_core.messages import HumanMessage
from langgraph_recipes.common.simple_graph_agent import Agent
from langgraph_recipes.tools.calculator import Calculator
from utils.model_selector import use_model, ModelType

# Initialize the calculator tool
calculator = Calculator()

# Initialize the model
model = use_model(ModelType.OPENAI)

# Create system prompt for the calculator agent
prompt = """You are a helpful calculator assistant. You can perform basic arithmetic operations.
When asked a math question, use the calculator tool to compute the result.
Always show your work by using the calculator for each step."""

# Create the agent with the calculator tool
calc_agent = Agent(model, [calculator], system=prompt)

# Example usage
messages = [HumanMessage(content="What is 25 + (15 multiplied by 2)?")]
result = calc_agent.graph.invoke({'messages': messages})

# Print the result
print("\nConversation:")
for message in result['messages']:
    print(f"{message.type}: {message.content}")