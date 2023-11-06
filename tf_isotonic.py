import os
import tensorflow as tf

from typing import Optional, Tuple

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "1"
tf.get_logger().setLevel("ERROR")


class TFIsotonicRegression(tf.keras.Model):
    def __init__(
        self,
        y_min: Optional[float] = None,
        y_max: Optional[float] = None,
        increasing: bool = True,
        approximation: int = -1,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.y_min = y_min
        self.y_max = y_max
        self.increasing = increasing
        self.approximation = min(max(approximation, -1), 10)
        self.verbose = False
        self.accumulated_X = tf.constant([], dtype=tf.float32)
        self.accumulated_y = tf.constant([], dtype=tf.float32)

    @staticmethod
    @tf.function
    def _to_matrix(x1: tf.Tensor, x2: tf.Tensor) -> tf.Tensor:
        x1, x2 = [
            tf.expand_dims(t, axis=-1) if len(t.shape) == 1 else t for t in [x1, x2]
        ]
        return tf.concat((x1, x2), axis=-1)

    @staticmethod
    @tf.function
    def _check_rank(x: tf.Tensor) -> tf.Tensor:
        condition = tf.math.less_equal(tf.rank(x), 2)
        msg = "Only 1d tensors or 2d tensors with one feature are supported."
        tf.debugging.assert_equal(condition, tf.constant(True), message=msg)

        try:
            # @tf.function is unable to identify rank of tensor correctly
            return tf.squeeze(x, axis=1)
        except ValueError:
            return x

    @staticmethod
    @tf.function
    def _in1d(arr1: tf.Tensor, arr2: tf.Tensor) -> tf.Tensor:
        """Returns boolean as tf.Tensor"""
        arr1 = tf.expand_dims(arr1, 1)
        arr2 = tf.expand_dims(arr2, 0)
        return tf.reduce_any(tf.reduce_any(tf.equal(arr1, arr2), axis=1))

    @tf.function
    def _tf_groupby(self, x1: tf.Tensor, x2: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:
        tensor = self._to_matrix(x1, x2)
        sorted_indices = tf.argsort(tensor[:, 0])
        sorted_tensor = tf.gather(tensor, sorted_indices)

        unique_keys, idx, counts = tf.unique_with_counts(sorted_tensor[:, 0])
        sum_values = tf.math.unsorted_segment_sum(
            sorted_tensor[:, 1], idx, tf.shape(unique_keys)[0]
        )
        aggregated_values = sum_values / tf.cast(counts, tf.float32)

        return unique_keys, aggregated_values

    @staticmethod
    @tf.function
    def _linear_interp1d(x: tf.Tensor, y: tf.Tensor, new_x: tf.Tensor) -> tf.Tensor:
        indices_below = tf.searchsorted(x, new_x, side="right") - 1
        indices_below = tf.clip_by_value(indices_below, 0, tf.shape(x)[0] - 2)

        x_below = tf.gather(x, indices_below)
        y_below = tf.gather(y, indices_below)

        x_above = tf.gather(x, indices_below + 1)
        y_above = tf.gather(y, indices_below + 1)

        slope = (y_above - y_below) / (x_above - x_below)
        interpolated_y = y_below + slope * (new_x - x_below)

        return interpolated_y

    def _build_f(self, X: tf.Tensor, y: tf.Tensor) -> None:
        if len(y) == 1:
            self.f_ = lambda x: tf.repeat(y, len(X))
        else:
            self.f_ = lambda x: self._linear_interp1d(X, y, x)

    @tf.function
    def _build_y(
        self, X: tf.Tensor, y: tf.Tensor
    ) -> Tuple[tf.Tensor, tf.Tensor, tf.Tensor, tf.Tensor]:
        iso_y = tf.clip_by_value(
            tf.nn.isotonic_regression(y, decreasing=not self.increasing)[0],
            self.y_min,
            self.y_max,
        )

        X_min_, X_max_ = tf.reduce_min(X), tf.reduce_max(X)
        return X, iso_y, X_min_, X_max_

    @tf.function
    def _fit(self, X: tf.Tensor, y: tf.Tensor) -> None:
        unique_X, unique_y = self._tf_groupby(X, y)
        X_iso, y_iso, self.X_min_, self.X_max_ = self._build_y(unique_X, unique_y)
        self._build_f(X_iso, y_iso)

    def _fit_batch(self, X: tf.Tensor, y: tf.Tensor, build_final: bool) -> None:
        combined_X = tf.concat([self.accumulated_X, X], axis=0)
        combined_y = tf.concat([self.accumulated_y, y], axis=0)

        if self._in1d(self.accumulated_X, X) and not build_final:
            self.accumulated_X, self.accumulated_y = self._tf_groupby(
                combined_X, combined_y
            )

        if build_final:
            self.accumulated_X, self.accumulated_y = self._tf_groupby(
                combined_X, combined_y
            )
            X_iso, y_iso, self.X_min_, self.X_max_ = self._build_y(
                self.accumulated_X, self.accumulated_y
            )
            self._build_f(X_iso, y_iso)

    def _fit_batchwise(self, X: tf.Tensor, y: tf.Tensor, batch_size: int) -> None:
        num_batches = len(X) // batch_size
        dataset = tf.data.Dataset.from_tensor_slices((X, y)).batch(batch_size)

        for i, (batch_X, batch_y) in enumerate(dataset):
            if i >= num_batches - 1:
                self._fit_batch(batch_X, batch_y, build_final=True)
            else:
                self._fit_batch(batch_X, batch_y, build_final=False)

            self._print_progressbar(i, num_batches)

    def _print_progressbar(self, i: int, max_value: int) -> None:
        if self.verbose:
            progress_percentage = (i + 1) * 100.0 / max_value
            num_arrows = int(progress_percentage // 5)
            progress_bar = ["▩"] * num_arrows + ["_"] * (20 - num_arrows)
            tf.print(
                "\rProgress: [",
                "".join(progress_bar),
                "] ",
                f" [{i}/{max_value}]",
                sep="",
                end="",
            )

    def fit(
        self,
        x=None,
        y=None,
        batch_size=None,
        epochs=1,
        verbose=False,
        callbacks=None,
        validation_split=0.0,
        validation_data=None,
        shuffle=True,
        class_weight=None,
        sample_weight=None,
        initial_epoch=0,
        steps_per_epoch=None,
        validation_steps=None,
        validation_batch_size=None,
        validation_freq=1,
        max_queue_size=10,
        workers=1,
        use_multiprocessing=False,
    ) -> tf.keras.Model:
        """
        Parameters except x, y, batch_size and verbose do nothing and serve only the compatibility purpose
        """
        self.verbose = verbose

        X = self._check_rank(tf.convert_to_tensor(x, dtype=tf.float32))
        y = self._check_rank(tf.convert_to_tensor(y, dtype=tf.float32))

        if self.approximation > -1:
            multiplier = tf.constant(10**self.approximation, dtype=X.dtype)
            X = tf.round(X * multiplier) / multiplier

        if batch_size is None:
            self._fit(X, y)
        else:
            self._fit_batchwise(X, y, batch_size)

        return self

    @tf.function
    def call(self, T: tf.Tensor) -> tf.Tensor:
        assert hasattr(self, "f_"), "Model is not fitted"
        T = self._check_rank(tf.convert_to_tensor(T, dtype=tf.float32))
        T = tf.clip_by_value(T, self.X_min_, self.X_max_)
        return self.f_(T)

    def __repr__(self) -> str:
        return (
            f"TFIsotonicRegression(y_min={self.y_min}, y_max={self.y_max}, increasing={self.increasing}, "
            f"approximation={self.approximation})"
        )
