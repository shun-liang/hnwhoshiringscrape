import json
import requests

from concurrent.futures import ThreadPoolExecutor


THREAD_POOL_SIZE = 15


with open('posts.json') as posts_file:
    POSTS_JSON = json.load(posts_file)
    JOB_POST_POINTERS = POSTS_JSON['pointers']
    NON_JOB_POSTS = POSTS_JSON['non_job_post']


def get_all_whoishring_root_posts():
    user_request = requests.get('https://hacker-news.firebaseio.com/v0/user/whoishiring.json')
    if user_request.status_code == 200:
        root_posts = user_request.json()['submitted']
    user_request = requests.get('https://hacker-news.firebaseio.com/v0/user/_whoishiring.json')
    if user_request.status_code == 200:
        root_posts += user_request.json()['submitted']
    print('root_posts: %s' % root_posts)
    return root_posts


def scrape_jobs(root_post_id):
    posts = {
        'root_post_id': root_post_id,
        'job_posts': [],
    }
    executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
    futures = []
    if str(root_post_id) in JOB_POST_POINTERS:
        root_post_id = JOB_POST_POINTERS[str(root_post_id)]
    if root_post_id in NON_JOB_POSTS:
        print('%s is not a job post' % root_post_id)
        return None
    root_post_request = requests.get('https://hacker-news.firebaseio.com/v0/item/%s.json' %
                                     str(root_post_id))
    root_post_json = root_post_request.json()
    if 'time' not in root_post_json:
        print('time attribute not in post %s' % root_post_id)
        return None
    posts['time'] = root_post_json['time']
    if 'title' in root_post_json:
        root_post_title = root_post_json['title']
        if "hiring" in root_post_title.lower():
            print('%s: %s' % (root_post_title, root_post_request.url))
            job_post_ids = root_post_json['kids']
            for job_post_id in job_post_ids:
                task_future = executor.submit(append_job_to_list, posts['job_posts'], job_post_id)
                futures.append(task_future)
    else:
        print('Can\'t get attribute \'title\' from root post %s' % root_post_request.url)
    for task_future in futures:
        task_future.result()
    return posts


def append_job_to_list(_list, job_post_id):
    _list.append(request_post_content(job_post_id))


def request_post_content(job_post_id):
    post_request = requests.get('https://hacker-news.firebaseio.com/v0/item/%s.json' % str(job_post_id))
    post_json = post_request.json()
    post_text = post_json.get('text')
    return {
        'job_post_id': job_post_id,
        'text': post_text,
    }


def main():
    output = {
        'root_posts': [],
    }
    for root_post in get_all_whoishring_root_posts()[0:3]:
        jobs = scrape_jobs(root_post)
        output['root_posts'].append(jobs)
    with open('jobs.json', 'w') as output_file:
        output_file.write(json.dumps(output))

main()
