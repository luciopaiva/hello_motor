

def bar():
    yield from range(10)


def foo():
    _g = bar()
    print(_g)
    # for i in bar():
    #     print(i)


if __name__ == '__main__':
    foo()
