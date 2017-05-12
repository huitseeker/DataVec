package org.datavec.api.transform.utils;

/**
 * Created by huitseeker on 5/12/17.
 */
public abstract class HList<A extends HList<A>> {
    private HList() {}

    private static final HNil nil = new HNil();

    public static HNil nil() {
        return nil;
    }

    public static <E, L extends HList<L>> HCons<E, L> cons(final E e, final L l) {
        return new HCons<E, L>(e, l);
    }

    public static final class HNil extends HList<HNil> {
        private HNil() {}
    }

    public static final class HCons<E, L extends HList<L>> extends HList<HCons<E, L>> {
        private E e;
        private L l;

        private HCons(final E e, final L l) {
            this.e = e;
            this.l = l;
        }

        public E head() {
            return e;
        }

        public L tail() {
            return l;
        }
    }
}