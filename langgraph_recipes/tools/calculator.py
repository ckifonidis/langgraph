from typing import Dict, Optional, Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


class CalculatorInput(BaseModel):
    """Input for the calculator tool."""
    operation: str = Field(description="The math operation to perform (+, -, *, /)")
    a: float = Field(description="First number")
    b: float = Field(description="Second number")


class Calculator(BaseTool):
    """Tool that performs basic arithmetic operations."""
    name: str = "calculator"
    description: str = "Useful for performing basic arithmetic operations"
    args_schema: Type[BaseModel] = CalculatorInput

    def _run(self, operation: str, a: float, b: float) -> str:
        """Run the calculator tool."""
        try:
            if operation == "+":
                result = a + b
            elif operation == "-":
                result = a - b
            elif operation == "*":
                result = a * b
            elif operation == "/":
                if b == 0:
                    return "Error: Division by zero"
                result = a / b
            else:
                return f"Error: Invalid operation '{operation}'. Use +, -, *, or /"
            
            return f"{a} {operation} {b} = {result}"
        except Exception as e:
            return f"Error: {str(e)}"