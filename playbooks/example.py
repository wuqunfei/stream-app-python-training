import faust

app = faust.App('example')


@app.agent()
async def foo(stream):
    async for value in stream:
        await bar.send(value)
        yield value


@app.agent()
async def bar(stream):
    async for value in stream:
        yield value + 'YOLO'

