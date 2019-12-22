package com.ajjpj.asqlmapper.mapper.util;

import static com.ajjpj.asqlmapper.testutil.CollectionUtils.setOf;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import com.ajjpj.asqlmapper.mapper.util.sub.MtdVisibilitySubOtherPkg;
import org.junit.jupiter.api.Test;

public class BeanReflectionHelperTest {
    @Test void testAllSuperTypes() {
        assertEquals(setOf(Object.class), BeanReflectionHelper.allSuperTypes(Object.class));
        assertEquals(setOf(SubClass.class, Object.class), BeanReflectionHelper.allSuperTypes(SubClass.class));
        assertEquals(setOf(SubSubClass.class, SubClass.class, Object.class), BeanReflectionHelper.allSuperTypes(SubSubClass.class));

        assertEquals(setOf(SubClassWithIface.class, IFaceWithExtends.class, Serializable.class, Object.class),
                BeanReflectionHelper.allSuperTypes(SubClassWithIface.class));
        assertEquals(setOf(SubSubClassWithIface.class, Runnable.class, SubClassWithIface.class, IFaceWithExtends.class, Serializable.class, Object.class),
                BeanReflectionHelper.allSuperTypes(SubSubClassWithIface.class));
    }

    static class SubClass {}
    static class SubSubClass extends SubClass {}

    static class SubClassWithIface implements IFaceWithExtends {}
    static class SubSubClassWithIface extends SubClassWithIface implements Runnable {
        @Override public void run() {}
    }

    interface IFaceWithExtends extends Serializable {}

    @Test void testAllSuperMethodsSimple() throws Exception {
        assertEquals(setOf(MtdSimple.class.getMethod("getName")), BeanReflectionHelper.allSuperMethods(MtdSimple.class, MtdSimple.class.getMethod("getName")));
        assertEquals(setOf(MtdSimple.class.getMethod("setName", String.class)), BeanReflectionHelper.allSuperMethods(MtdSimple.class, MtdSimple.class.getMethod("setName", String.class)));
        assertEquals(setOf(MtdSimple.class.getMethod("twoParams", int.class, String.class)), BeanReflectionHelper.allSuperMethods(MtdSimple.class, MtdSimple.class.getMethod("twoParams", int.class, String.class)));
    }

    @Test void testAllSuperMethodsOnyInSuperclass() throws Exception {
        assertEquals(setOf(Object.class.getMethod("getClass")), BeanReflectionHelper.allSuperMethods(MtdSimple.class, MtdSimple.class.getMethod("getClass")));
    }

    @Test void testAllSuperMethodsOverride() throws Exception {
        assertEquals(setOf(MtdSimple.class.getMethod("getName"), MtdSimpleSub.class.getMethod("getName")),
                BeanReflectionHelper.allSuperMethods(MtdSimpleSub.class, MtdSimpleSub.class.getMethod("getName")));
        assertEquals(setOf(MtdSimple.class.getMethod("setName", String.class), MtdSimpleSub.class.getMethod("setName", String.class)),
                BeanReflectionHelper.allSuperMethods(MtdSimpleSub.class, MtdSimple.class.getMethod("setName", String.class)));
        assertEquals(setOf(MtdSimple.class.getMethod("twoParams", int.class, String.class), MtdSimpleSub.class.getMethod("twoParams", int.class, String.class)),
                BeanReflectionHelper.allSuperMethods(MtdSimpleSub.class, MtdSimple.class.getMethod("twoParams", int.class, String.class)));
    }

    @Test void testAllSuperMethodsClassAndIface() throws Exception {
        assertEquals(setOf(MtdSimple.class.getDeclaredMethod("getName"), MtdSimpleIface.class.getDeclaredMethod("getName")),
                BeanReflectionHelper.allSuperMethods(MtdClassAndIface.class, MtdClassAndIface.class.getMethod("getName")));
        assertEquals(setOf(MtdSimple.class.getDeclaredMethod("setName", String.class), MtdSimpleIface.class.getDeclaredMethod("setName", String.class)),
                BeanReflectionHelper.allSuperMethods(MtdClassAndIface.class, MtdClassAndIface.class.getMethod("setName", String.class)));
        assertEquals(setOf(MtdSimple.class.getDeclaredMethod("twoParams", int.class, String.class), MtdSimpleIface.class.getDeclaredMethod("twoParams", int.class, String.class)),
                BeanReflectionHelper.allSuperMethods(MtdClassAndIface.class, MtdClassAndIface.class.getMethod("twoParams", int.class, String.class)));
    }

    @Test void testAllSuperMethodsAbstractClass() throws Exception {
        assertEquals(setOf(MtdAbstract.class.getMethod("getName"), MtdExtendsAbstract.class.getMethod("getName")),
                BeanReflectionHelper.allSuperMethods(MtdExtendsAbstract.class, MtdExtendsAbstract.class.getMethod("getName")));
        assertEquals(setOf(MtdAbstract.class.getMethod("setName", String.class), MtdExtendsAbstract.class.getMethod("setName", String.class)),
                BeanReflectionHelper.allSuperMethods(MtdExtendsAbstract.class, MtdExtendsAbstract.class.getMethod("setName", String.class)));
        assertEquals(setOf(MtdAbstract.class.getMethod("twoParams", int.class, String.class), MtdExtendsAbstract.class.getMethod("twoParams", int.class, String.class)),
                BeanReflectionHelper.allSuperMethods(MtdExtendsAbstract.class, MtdExtendsAbstract.class.getMethod("twoParams", int.class, String.class)));
    }

    @Test void testAllSuperMethodsPrimitiveParams() throws Exception {
        assertEquals(setOf(MtdWidening.class.getDeclaredMethod("twoParams", long.class, String.class)),
                BeanReflectionHelper.allSuperMethods(MtdWidening.class, MtdWidening.class.getDeclaredMethod("twoParams", long.class, String.class)));
        assertEquals(setOf(MtdPrimitiveWrapper.class.getDeclaredMethod("twoParams", Integer.class, String.class)),
                BeanReflectionHelper.allSuperMethods(MtdPrimitiveWrapper.class, MtdPrimitiveWrapper.class.getDeclaredMethod("twoParams", Integer.class, String.class)));
    }

    @Test void testAllSuperMethodsIgnoreContravariantParameter() throws Exception {
        assertEquals(setOf(MtdParamContravariant.class.getDeclaredMethod("setName", CharSequence.class)),
                BeanReflectionHelper.allSuperMethods(MtdParamContravariant.class, MtdParamContravariant.class.getDeclaredMethod("setName", CharSequence.class)));
    }

    @Test void testAllSuperMethodsVisibility() throws Exception {
        assertEquals(setOf(MtdVisibilitySuper.class.getDeclaredMethod("prot"), MtdVisibilitySubSamePkg.class.getDeclaredMethod("prot")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubSamePkg.class, MtdVisibilitySubSamePkg.class.getDeclaredMethod("prot")));
        assertEquals(setOf(MtdVisibilitySuper.class.getDeclaredMethod("pkg"), MtdVisibilitySubSamePkg.class.getDeclaredMethod("pkg")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubSamePkg.class, MtdVisibilitySubSamePkg.class.getDeclaredMethod("pkg")));
        assertEquals(setOf(MtdVisibilitySubSamePkg.class.getDeclaredMethod("priv")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubSamePkg.class, MtdVisibilitySubSamePkg.class.getDeclaredMethod("priv")));

        assertEquals(setOf(MtdVisibilitySuper.class.getDeclaredMethod("prot"), MtdVisibilitySubOtherPkg.class.getDeclaredMethod("prot")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubOtherPkg.class, MtdVisibilitySubOtherPkg.class.getDeclaredMethod("prot")));
        assertEquals(setOf(MtdVisibilitySubOtherPkg.class.getDeclaredMethod("pkg")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubOtherPkg.class, MtdVisibilitySubOtherPkg.class.getDeclaredMethod("pkg")));
        assertEquals(setOf(MtdVisibilitySubOtherPkg.class.getDeclaredMethod("priv")),
                BeanReflectionHelper.allSuperMethods(MtdVisibilitySubOtherPkg.class, MtdVisibilitySubOtherPkg.class.getDeclaredMethod("priv")));
    }

    static class MtdSimple {
        public CharSequence getName() {return null;}
        public void setName(String s) {}
        public int twoParams(int l, String s) {return 0;}
    }

    static class MtdSimpleSub extends MtdSimple {
        public CharSequence getName() {return null;}
        public void setName(String s) {}
        public int twoParams(int l, String s) {return 0;}
    }

    interface MtdSimpleIface {
        CharSequence getName();
        void setName(String s);
        int twoParams(int l, String s);
    }

    static class MtdClassAndIface extends MtdSimple implements MtdSimpleIface {
    }

    abstract static class MtdAbstract {
        public abstract CharSequence getName();
        public abstract void setName(String s);
        public abstract int twoParams(int l, String s);
    }

    static class MtdExtendsAbstract extends MtdAbstract {
        public CharSequence getName() {return null;}
        public void setName(String s) {}
        public int twoParams(int l, String s) {return 0;}
    }

    static class MtdWidening extends MtdSimple {
        public int twoParams(long l, String s) {return 0;}
    }
    static class MtdPrimitiveWrapper extends MtdSimple {
        public int twoParams(Integer l, String s) {return 0;}
    }
    static class MtdParamContravariant extends MtdSimple {
        public void setName(CharSequence s) {}
    }

    public static class MtdVisibilitySuper {
        protected void prot() {}
        void pkg() {}
        private void priv() {}
    }

    static class MtdVisibilitySubSamePkg extends MtdVisibilitySuper {
        protected void prot() {}
        void pkg() {}
        void priv() {}
    }

    @Test void testElementType() throws Exception {
        assertEquals(String.class, BeanReflectionHelper.elementType(getClass().getDeclaredMethod("stringList").getGenericReturnType()));
        assertEquals(Integer.class, BeanReflectionHelper.elementType(getClass().getDeclaredMethod("integerSet").getGenericReturnType()));

        assertThrows(IllegalArgumentException.class, () -> BeanReflectionHelper.elementType(String.class));
        assertThrows(IllegalArgumentException.class, () -> BeanReflectionHelper.elementType(getClass().getDeclaredMethod("listSet").getGenericReturnType()));
    }

    List<String> stringList() {return null;}
    Set<Integer> integerSet() {return null;}
    Set<List<Integer>> listSet() {return null;}

    @Test void testUnchecked() {
        assertEquals("abc", BeanReflectionHelper.unchecked(() -> "abc"));
        assertEquals("xyz", BeanReflectionHelper.unchecked(() -> "xyz"));

        assertThrows(IOException.class, () -> BeanReflectionHelper.unchecked(() -> {
            throw new IOException();
        }));

        assertThrows(IOException.class, () -> BeanReflectionHelper.unchecked(() -> {
            throw new InvocationTargetException(new IOException());
        }));
    }
}
