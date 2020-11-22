import ipaddress
import aiohttp
import unittest


async def fetch():
    async with aiohttp.ClientSession() as session:
        async with session.get(url="https://api.ip.sb/jsonip") as response:
            response = await response.json()
    return response


class AsyncTest(unittest.IsolatedAsyncioTestCase):

    async def test_fetch(self):
        response = await fetch()
        ip = response.get('ip')
        self.assertTrue(type(ipaddress.ip_address(ip)), ipaddress.IPv4Address)


if __name__ == "__main__":
    unittest.main()
