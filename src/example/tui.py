from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable
import asyncio
import datetime
import random

class LiveFeedApp(App):
    """A Textual app for streaming live feed in a columnar table."""

    CSS = """
    Vertical {
        height: 100%;
        width: 100%;
        padding: 1;
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        """Create the layout."""
        with Vertical():
            self.table = DataTable()
            self.table.add_columns("Name", "Avg", "IDCS", "SID", "Datetime")
            yield self.table

    async def on_mount(self) -> None:
        """Populate the table with initial data and set up live updates."""
        # Initial data
        self.data = [
            {"Name": "EFF", "Avg": "51.45", "IDCS": "FI8001", "SID": "8528", "Datetime": "2025-04-16 01:12:54"},
            {"Name": "INFLU", "Avg": "44.48", "IDCS": "M100FI", "SID": "2308", "Datetime": "2025-04-16 01:12:54"}
        ]

        # Populate the table
        for item in self.data:
            self.table.add_row(item["Name"], item["Avg"], item["IDCS"], item["SID"], item["Datetime"])

        # Start live updates
        self.set_interval(1.0, self.update_live_feed)


    async def update_live_feed(self) -> None:
        """Simulate live data updates."""
        # Simulating a new data feed
        new_data = {
            "Name": "NEW",
            "Avg": f"{round(40 + 10 * (await self.random_float()), 2)}",
            "IDCS": f"ID{await self.random_int()}",
            "SID": f"{await self.random_int()}",
            "Datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Add new row
        self.table.add_row(
            new_data["Name"],
            new_data["Avg"],
            new_data["IDCS"],
            new_data["SID"],
            new_data["Datetime"],
        )
    async def generate_multiple_random_floats():
        # Generate multiple random floats asynchronously
        results = await asyncio.gather(self.random_float(), self.random_float(), self.random_float())
        return results
    
    async def random_float(self):
        return random.random()  # Returns a random float between 0.0 and 1.0

    async def random_int(self, min_val=1000, max_val=9999):
        # Simulate an async delay (if needed)
        await asyncio.sleep(0.1)  # Mimics async behavior, can be omitted if not required
        return random.randint(min_val, max_val)  # Returns a random integer between min_val and max_val
    
    def run(self):
        print("what can go here?")
        super().run()

    def run_logging(self):
        import logging
        logging.basicConfig(level=logging.INFO)  # Set up logging
        logging.info("Starting LiveFeedApp...")
        super().run()  # Call the parent class's run method to launch the app

    def run_config(self, config_file=None):
        if config_file:
            # Load configuration (mock example)
            print(f"Loading configuration from {config_file}")
        super().run()


if __name__ == "__main__":
    app = LiveFeedApp()
    app.run_logging()