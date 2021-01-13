# %%
from coroflow import Node, Pipeline
import time


def sync_image_scraper():
    time.sleep(1)
    yield 'img_url_1'
    time.sleep(1)
    yield 'img_url_2'
    time.sleep(1)
    yield 'img_url_3'


def func1(targets, param=None):
    print(f"func1: Got param: {param}")
    while True:
        inpt = (yield)
        time.sleep(1)  # simulated IO delay
        for target in targets:
            print(f"func1: T1 sending {inpt}")
            target.send(inpt)


def func2(targets, param=None):
    print(f"func2: Got param: {param}")
    while True:
        inpt = (yield)
        time.sleep(1)  # simulated IO delay
        print(f"func2: Got sent value {inpt}")


p = Pipeline()

t1 = Node('func1', p, coro_func=func1, kwargs={'param': 'param_t1'})
t2 = Node('func2', p, coro_func=func2, kwargs={'param': 'param_t2'})
t1.set_downstream(t2)


# %%
def scrape_and_process(scraper, pipeline):
    pipeline.build()
    pipeline.render()
    pipeline_root = pipeline.root_coro
    for url in scraper():
        print(f'URL is {url}')
        pipeline_root.send(url)

# %%
start_time = time.time()
scrape_and_process(sync_image_scraper, p)
print(f"Synchronous duration: {time.time() - start_time}s.")
