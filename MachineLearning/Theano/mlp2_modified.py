 
# start-snippet-2
class MLP(object): 

    def __init__(self, rng, input, n_in, n_hidden, n_out):
        """Initialize the parameters for the multilayer perceptron with 2 hidden layer

         """
        self.hiddenLayer = HiddenLayer(
            rng=rng,
            input=input,
            n_in=n_in,
            n_out=n_hidden,
            activation=T.tanh
        )

#create hidden layer 2 that take hidden layer as input, and output to log layer 
        self.hiddenLayer2 = HiddenLayer(
            rng=rng,
            input=self.hiddenLayer.output,
            n_in=n_hidden,
            n_out=n_hidden,
            activation=T.tanh
        )


        # The logistic regression layer gets hidden layer 2 as input
        self.logRegressionLayer = LogisticRegression(
            input=self.hiddenLayer2.output,
            n_in=n_hidden,
            n_out=n_out
        )

        #include hiddenlayer2 in regularization
        self.L1 = (
            abs(self.hiddenLayer.W).sum()
            + abs(self.hiddenLayer2.W).sum()
            + abs(self.logRegressionLayer.W).sum()
        )

        #include hiddenlayer2 in regularization
        self.L2_sqr = (
            (self.hiddenLayer.W ** 2).sum()
            + (self.hiddenLayer2.W ** 2).sum()
            + (self.logRegressionLayer.W ** 2).sum()
        )

        self.negative_log_likelihood = (
            self.logRegressionLayer.negative_log_likelihood
        )

        self.errors = self.logRegressionLayer.errors

        #include hiddenlayer2 as parameter
        self.params = self.hiddenLayer.params + self.hiddenLayer2.params + self.logRegressionLayer.params

        self.input = input
 