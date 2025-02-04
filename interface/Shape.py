class Shape:
    def __init__(self):
        pass

    def valid(self):
        """这个方法应该在子类中被重写以检查特定形态的特征"""
        raise NotImplementedError("This method should be overridden by subclasses.")
