import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock

from playbooks.test.example import app, bar, foo


class Test(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        app.finalize()
        app.conf.store = 'memory://'
        app.flow_control.resume()
        return app

    async def test_bar(self):
        async with bar.test_context() as agent:
            event = await agent.put("hey")
            self.assertEqual(agent.results[event.message.offset], 'heyYOLO')

    async def test_foo(self):
        message = "hey"
        with patch('playbooks.example.bar', new_callable=AsyncMock) as mocked_bar:
            mocked_bar.return_value = None
            async with foo.test_context() as agent:
                await agent.put(message)
                self.assertIsNone(mocked_bar.send.assert_awaited_with(message))


if __name__ == "__main__":
    unittest.main()
