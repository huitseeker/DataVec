package org.datavec.api.transform.ops;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by huitseeker on 5/1/17.
 */
public abstract class DecoratedBiFunction<T, U, R> implements BiFunction<T, U, R> {
    public abstract R apply(T var1, U var2);

    @Override
    public <V> BiFunction<T, U, V> andThen(final Function<? super R, ? extends V> var1) {
        Objects.requireNonNull(var1);
        return new DecoratedBiFunction<T, U ,V>(){
            @Override
        public V apply(T var2, U var3){
                return var1.apply(DecoratedBiFunction.this.apply(var2, var3));
            }
        };
    }
}