from langchain_core.messages import HumanMessage
from utils.model_selector import use_model, ModelType
from langgraph_recipes.common.simple_graph_agent import Agent
from langgraph_recipes.tools.file_tools import ListFilesTool

def main():
    # Initialize the list files tool
    list_files_tool = ListFilesTool()
    
    # Initialize the model
    model = use_model(ModelType.OPENAI)
    
    # Create the agent with our tool
    prompt = """You are a helpful assistant that can list files in directories.
    When asked about files, use the list_files tool to show the contents of directories.
    If a specific type of file is requested, use the pattern parameter to filter the results."""
    
    agent = Agent(model, [list_files_tool], system=prompt)
    
    # Example usage
    messages = [
        HumanMessage(content="Show me all SQL files under langgraph_recipes/table_descriptions")
    ]
    
    # Run the agent
    result = agent.graph.invoke({'messages': messages})
    print("Agent execution completed!")

if __name__ == "__main__":
    main()