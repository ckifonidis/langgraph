from utils import model_selector
from pprint import pprint
from langgraph_recipes.common.agent_simple import Agent

def main():
    model = model_selector.use_model(model_selector.ModelType.OPENAI)
    agent = Agent(model)
    
    print("Start chatting with the AI (type 'exit' to end the conversation)")
    
    while True:
        user_input = input("\nYou: ").strip()
        
        if user_input.lower() == 'exit':
            print("\nGoodbye!")
            break
            
        if user_input:
            response = agent(user_input)
            print("\nAI:", response)
        else:
            print("Please enter a message")

if __name__ == "__main__":
    main()