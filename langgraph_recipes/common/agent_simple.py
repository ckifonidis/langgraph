from pprint import pprint

class Agent:
    def __init__(self, model, system=""):
        self.model = model
        self.system = system
        self.messages = []
        if self.system:
            self.messages.append([
                ("system", system)
            ])

    def __call__(self, message):
        self.messages.append(("human", message))
        result = self.execute()
        self.messages.append(("ai", message))
        return result

    def execute(self):
        result = self.model.invoke(self.messages)
        return result.content