from pprint import pprint
from typing import List, Dict, Optional
from langgraph.graph import StateGraph, END
from langgraph_recipes.common.agent_state import AgentState
from langchain_core.messages import SystemMessage, ToolMessage, AIMessage, BaseMessage
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool

class Agent:

    def __init__(
        self,
        model: BaseChatModel,
        tools: List[BaseTool],
        system: str = ""
    ) -> None:
        self.system: str = system
        graph = StateGraph(AgentState)
        graph.add_node("llm", self.call_openai)
        graph.add_node("llm2", self.call_openai)
        graph.add_node("action", self.take_action)
        graph.add_conditional_edges(
            "llm",
            self.exists_action,
            {True: "action", False: END}
        )
        graph.add_edge("action", "llm")
        graph.set_entry_point("llm")
        self.graph = graph.compile()
        self.tools: Dict[str, BaseTool] = {t.name: t for t in tools}
        self.model = model.bind_tools(tools)

    def exists_action(self, state: AgentState) -> bool:
        result = state['messages'][-1]
        if isinstance(result, AIMessage):
            return len(result.tool_calls) > 0
        return False

    def call_openai(self, state: AgentState) -> Dict[str, List[BaseMessage]]:
        messages = state['messages']
        if self.system:
            messages = [SystemMessage(content=self.system)] + messages
        message = self.model.invoke(messages)
        return {'messages': [message]}

    def take_action(self, state: AgentState) -> Dict[str, List[BaseMessage]]:
        last_message = state['messages'][-1]
        if not isinstance(last_message, AIMessage):
            return {'messages': []}
            
        tool_calls = last_message.tool_calls
        results = []
        for t in tool_calls:
            print(f"Calling: {t}")
            if not t['name'] in self.tools:      # check for bad tool name from LLM
                print("\n ....bad tool name....")
                result = "bad tool name, retry"  # instruct LLM to retry if bad
            else:
                result = self.tools[t['name']].invoke(t['args'])
            results.append(ToolMessage(tool_call_id=t['id'], name=t['name'], content=str(result)))
        print("Back to the model!")
        pprint(results)
        return {'messages': results}