from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock, Mock

from playbooks.example import app, bar, foo


class Test(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        app.finalize()
        app.conf.store = 'memory://'
        app.flow_control.resume()
        return app

    async def asyncTearDown(self) -> None:
        return await super().asyncTearDown()

    async def test_bar(self):
        async with bar.test_context() as agent:
            event = await agent.put("hey")
            self.assertEqual(agent.results[event.message.offset], 'heyYOLO')

    async def test_foo(self):
        with patch('playbooks.example.bar') as mocked_bar:
            mocked_bar.send = mock_coro()
            async with foo.test_context() as agent:
                await agent.put('hey')
                mocked_bar.send.assert_called_with('hey')


def mock_coro(return_value=None, **kwargs):
    """Create mock coroutine function."""

    async def wrapped(*args, **kwargs):
        return return_value

    return Mock(wraps=wrapped, **kwargs)
