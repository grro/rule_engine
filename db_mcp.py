from mcp_server import MCPServer
from device import Store


class StoreMCPServer(MCPServer):

    def __init__(self, name: str, port: int, store: Store):
        super().__init__(name, port)
        self.store = store

        @self.mcp.resource("db://list/names")
        def list_names() -> str:
            """Lists all property names in the store"""
            return ", ".join(self.store.property_names)

        @self.mcp.resource("db://{name}")
        def get_value(name: str) -> str:
            """Ges a property in the store"""
            try:
                return str(self.store.get_property(name))
            except Exception as e:
                return f"Error retrieving {name}: {str(e)}"

        @self.mcp.tool()
        def set_value(name: str, value: str) -> str:
            """Updates a property in the store with a new value."""
            try:
                old_value = self.store.get_property(name)
                if type(old_value) == bool:
                    value = True if value in {"true", 'True', 'TRUE'} else False
                elif type(old_value) == int:
                    value = int(value)
                self.store.set_property(name, value)
                return f"Successfully set {name} to {value}"
            except Exception as e:
                return f"Failed to update {name}: {str(e)}"

# npx @modelcontextprotocol/inspector

