import ast
from typing import List

class SanityCheckerASTVisitor(ast.NodeVisitor):
    """
    Helper Classes for AST Parsing
    """
    def __init__(self, flag_constructs: List) -> None:
        """
        Args:
            flag_constructs: List[str] - List of strings for which submission must be flagged.
        """

        self.errors = {}
        self.flag_constructs = flag_constructs
        for construct in flag_constructs:
            if construct not in self.errors:
                self.errors[construct] = []
        self.num_errors = 0

    def reset(self):
        self.num_errors = 0

        for k in self.errors:
            self.errors[k] = []
    
    def craft_error(self, message: str, line_number: int, col_offset: int, **kwargs):
        error_message = {}

        error_message["message"] = message
        error_message["lineno"] = line_number
        error_message["col_offset"] = col_offset

        if kwargs is not None:
            for key,value in kwargs.items():
                error_message[key] = value

        self.num_errors += 1
        return error_message
    
    def visit_For(self, node: ast.For):
        if "for" in self.flag_constructs:
            message = "For Loop Detected. No looping constructs are allowed in this assignment."
            lineno = node.lineno
            col_offset = node.col_offset
            
            error_message = self.craft_error(message, lineno, col_offset)
            self.errors["for"].append(error_message)
        
        self.generic_visit(node)

    def visit_While(self, node: ast.While):
        if "while" in self.flag_constructs:
            message = "While Loop Detected. No looping constructs are allowed in this assignment."
            lineno = node.lineno
            col_offset = node.col_offset
            
            error_message = self.craft_error(message, lineno, col_offset)
            self.errors["while"].append(error_message)
        
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name):
        if "open" in self.flag_constructs:
            if node.id == "open" and isinstance(node.ctx, ast.Load):
                message = "You are not allowed to use File operations for this assignment."
                lineno = node.lineno
                col_offset = node.col_offset

                error_message = self.craft_error(message, lineno, col_offset)
                self.errors["open"].append(error_message)
                
        self.generic_visit(node)

    def report(self) -> str:
        report = []

        for errors in self.errors:
            msg = ""
            line = "-" * len(errors)

            if len(self.errors[errors]) != 0:
                msg += f"{errors}\n{line}\n"

            for error_messages in self.errors[errors]:
                msg += f"Line: {error_messages['lineno']} Col: {error_messages['col_offset']} - {error_messages['message']}\n"
            
            report.append(msg)
        
        return "\n".join(report)