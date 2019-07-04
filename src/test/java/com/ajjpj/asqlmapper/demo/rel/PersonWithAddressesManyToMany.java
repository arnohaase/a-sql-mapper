package com.ajjpj.asqlmapper.demo.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.javabeans.annotations.ManyToMany;
import com.ajjpj.asqlmapper.javabeans.annotations.Table;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * Immutable implementation of {@link AbstractPersonWithAddresses}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code PersonWithAddresses.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code PersonWithAddresses.of()}.
 */
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
@CheckReturnValue
@Table("person")
public final class PersonWithAddressesManyToMany implements AbstractPersonWithAddressesManyToMany {
  private final Long id;
  private final String name;
  private final AList<Address> addresses;

  private PersonWithAddressesManyToMany(Long id, String name, AList<Address> addresses) {
    this.id = Objects.requireNonNull(id, "id");
    this.name = Objects.requireNonNull(name, "name");
    this.addresses = Objects.requireNonNull(addresses, "addresses");
  }

  private PersonWithAddressesManyToMany(
      PersonWithAddressesManyToMany original,
      Long id,
      String name,
      AList<Address> addresses) {
    this.id = id;
    this.name = name;
    this.addresses = addresses;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code addresses} attribute
   */
  @Override
  @ManyToMany(manyManyTable = "person_address")
  public AList<Address> addresses() {
    return addresses;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddresses#id() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddressesManyToMany withId(Long value) {
    Long newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return new PersonWithAddressesManyToMany(this, newValue, this.name, this.addresses);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddresses#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddressesManyToMany withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new PersonWithAddressesManyToMany(this, this.id, newValue, this.addresses);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddresses#addresses() addresses} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for addresses
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddressesManyToMany withAddresses(AList<Address> value) {
    if (this.addresses == value) return this;
    AList<Address> newValue = Objects.requireNonNull(value, "addresses");
    return new PersonWithAddressesManyToMany(this, this.id, this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code PersonWithAddresses} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof PersonWithAddressesManyToMany
        && equalTo((PersonWithAddressesManyToMany) another);
  }

  private boolean equalTo(PersonWithAddressesManyToMany another) {
    return id.equals(another.id)
        && name.equals(another.name)
        && addresses.equals(another.addresses);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code name}, {@code addresses}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + addresses.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code PersonWithAddresses} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("PersonWithAddresses")
        .omitNullValues()
        .add("id", id)
        .add("name", name)
        .add("addresses", addresses)
        .toString();
  }

  /**
   * Construct a new immutable {@code PersonWithAddresses} instance.
   * @param id The value for the {@code id} attribute
   * @param name The value for the {@code name} attribute
   * @param addresses The value for the {@code addresses} attribute
   * @return An immutable PersonWithAddresses instance
   */
  public static PersonWithAddressesManyToMany of(Long id, String name, AList<Address> addresses) {
    return new PersonWithAddressesManyToMany(id, name, addresses);
  }

  /**
   * Creates an immutable copy of a {@link AbstractPersonWithAddresses} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PersonWithAddresses instance
   */
  public static PersonWithAddressesManyToMany copyOf(AbstractPersonWithAddressesManyToMany instance) {
    if (instance instanceof PersonWithAddressesManyToMany) {
      return (PersonWithAddressesManyToMany) instance;
    }
    return PersonWithAddressesManyToMany.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link PersonWithAddressesManyToMany PersonWithAddresses}.
   * <pre>
   * PersonWithAddresses.builder()
   *    .id(Long) // required {@link AbstractPersonWithAddresses#id() id}
   *    .name(String) // required {@link AbstractPersonWithAddresses#name() name}
   *    .addresses(com.ajjpj.acollections.AList&amp;lt;de.Address&amp;gt;) // required {@link AbstractPersonWithAddresses#addresses() addresses}
   *    .build();
   * </pre>
   * @return A new PersonWithAddresses builder
   */
  public static PersonWithAddressesManyToMany.Builder builder() {
    return new PersonWithAddressesManyToMany.Builder();
  }

  /**
   * Builds instances of type {@link PersonWithAddressesManyToMany PersonWithAddresses}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_ADDRESSES = 0x4L;
    private long initBits = 0x7L;

    private @Nullable
    Long id;
    private @Nullable
    String name;
    private @Nullable
    AList<Address> addresses;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractPersonWithAddresses} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder from(AbstractPersonWithAddressesManyToMany  instance) {
      Objects.requireNonNull(instance, "instance");
      id(instance.id());
      name(instance.name());
      addresses(instance.addresses());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPersonWithAddresses#id() id} attribute.
     * @param id The value for id
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder id(Long id) {
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPersonWithAddresses#name() name} attribute.
     * @param name The value for name
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPersonWithAddresses#addresses() addresses} attribute.
     * @param addresses The value for addresses
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder addresses(AList<Address> addresses) {
      this.addresses = Objects.requireNonNull(addresses, "addresses");
      initBits &= ~INIT_BIT_ADDRESSES;
      return this;
    }

    /**
     * Builds a new {@link PersonWithAddressesManyToMany PersonWithAddresses}.
     * @return An immutable instance of PersonWithAddresses
     * @throws IllegalStateException if any required attributes are missing
     */
    public PersonWithAddressesManyToMany build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new PersonWithAddressesManyToMany(null, id, name, addresses);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_ADDRESSES) != 0) attributes.add("addresses");
      return "Cannot build PersonWithAddresses, some of required attributes are not set " + attributes;
    }
  }
}
