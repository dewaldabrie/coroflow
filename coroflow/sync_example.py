# %%
from coroflow import Task, Pipeline
import time


def sync_image_scraper():
    yield 'img_url_1'
    yield 'img_url_2'
    yield 'img_url_3'


p = Pipeline()

def func1(targets, param):
    print(f"func1: Got param: {param}")
    while True:
        inpt = (yield)
        time.sleep(1)  # simulated IO delay
        for target in targets:
            print(f"func1: T1 sending {inpt}")
            target.send(inpt)


def func2(targets, param):
    print(f"func2: Got param: {param}")
    while True:
        inpt = (yield)
        time.sleep(1)  # simulated IO delay
        print(f"func2: Got sent value {inpt}")


t1 = Task('foo', p, func1, args=('param_t1',))
t2 = Task('bar', p, func2, args=('param_t2',))
t1.set_downstream(t2)


# %%
def scrape_and_process(scraper, pipeline):
    pipeline.build()
    pipeline.render()
    pipeline_root = pipeline.root_task
    for url in scraper():
        print(f'URL is {url}')
        pipeline_root.send(url)

# %%
start_time = time.time()
scrape_and_process(sync_image_scraper, p)
print(f"Synchronous duration: {time.time() - start_time}s.")
