from langchain_core.messages import HumanMessage
from langchain_community.tools.tavily_search import TavilySearchResults

from langgraph_recipes.common.simple_graph_agent import Agent
from utils.model_selector import use_model, ModelType

tool = TavilySearchResults(max_results=4) #increased number of results



model = use_model(ModelType.OPENAI)

prompt = """You are a smart research assistant. Use the search engine to look up information. \
You are allowed to make multiple calls (either together or in sequence). \
Only look up information when you are sure of what you want. \
If you need to look up some information before asking a follow up question, you are allowed to do that!
"""

abot = Agent(model, [tool], system=prompt)
messages = [HumanMessage(content="What is the capital of France and the capital for Greece?")]
result = abot.graph.invoke({'messages': messages})