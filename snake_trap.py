from os import walk, sep
from os.path import join, isfile, abspath
from difflib import ndiff
from hashlib import md5
from multiprocessing import Pool, Manager


# Crawls through system getting the file names
def spyder(web):
    # Recursively crawls through file system
    # start from the root directory
    for root, dirs, files in walk(abspath(sep)):
        for name in files:
            full_path = join(root, name)
            # Some funny business goes on
            # if you don't check it's a file...
            if isfile(full_path):
                web.put(full_path)


# Copy one queue to another
def queue_copy(queue_one, queue_two):
    temp = []

    while not queue_one.empty():
        temp.append(queue_one.get())

    for item in temp:
        queue_one.put(item)
        queue_two.put(item)


# Adds the hashes of the files to the to_hashes queue
def get_hashes(to_hashes):
    temp = []

    while not to_hashes.empty():
        file_name = to_hashes.get()
        if isfile(file_name):
            temp.append(get_hash(open(file_name, 'rb'), md5()))

    for item in temp:
        to_hashes.put(item)


# Gets the MD5 hash of the file
# Most memory efficient way to do it
# DO NOT use hashlib.md5(open(full_path, 'rb').read()).hexdigest())
def get_hash(file_name, hasher, blocksize=65536):
    buff = file_name.read(blocksize)

    while len(buff) > 0:
        hasher.update(buff)
        buff = file_name.read(blocksize)

    return hasher.hexdigest()


# Writes the file names and hashes of the files to a txt file
# Precondition :: Assumes that web and hashes are parallel
def file_writer(file_name, web, hashes):
    with open(file_name, 'w') as writer:
        while not web.empty():
            while not hashes.empty():
                writer.write('{0}\t{1}\n'.format(web.get(), hashes.get()))


# Checks the difference of two files and prints out to terminal/prompt
def diff_check(file_one, file_two):
    for line in ndiff(
            open(file_one, 'r').readlines(), open(file_two, 'r').readlines()):
        if line.startswith('-'):
            print(line)
        elif line.startswith('+'):
            print '\t{0}' .format(line)


def main():
    # Queue manager to make the queues process safe
    manager = Manager()
    web = manager.Queue()
    hashes = manager.Queue()
    # Creates a multiprocess pool with with the number of CPU cores
    pool = Pool()

    if not isfile('baseline.txt'):
        # Runs the sypder function with web as an argument
        pool.map(spyder, (web, ))
        # map is synchronized so all process will wait until they're all done
        queue_copy(web, hashes)
        pool.map(get_hashes, (hashes, ))
        file_writer('baseline.txt', web, hashes)
    else:
        # Runs the sypder function with web as an argument
        pool.map(spyder, (web, ))
        # map is synchronized so all process will wait until they're all done
        queue_copy(web, hashes)
        pool.map(get_hashes, (hashes, ))
        file_writer('compare.txt', web, hashes)
        diff_check('baseline.txt', 'compare.txt')


if __name__ == '__main__':
    main()
