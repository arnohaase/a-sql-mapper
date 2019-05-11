package com.ajjpj.asqlmapper.demo.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import com.ajjpj.asqlmapper.javabeans.annotations.Table;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;

/**
 * Immutable implementation of {@link AbstractPersonWithAddress}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code PersonWithAddress.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code PersonWithAddress.of()}.
 */
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
@CheckReturnValue
@Table("person")
public final class PersonWithAddress implements AbstractPersonWithAddress {
  private final Long id;
  private final String name;
  private final Address address;

  private PersonWithAddress(Long id, String name, Address address) {
    this.id = Objects.requireNonNull(id, "id");
    this.name = Objects.requireNonNull(name, "name");
    this.address = Objects.requireNonNull(address, "address");
  }

  private PersonWithAddress(
      PersonWithAddress original,
      Long id,
      String name,
      Address address) {
    this.id = id;
    this.name = name;
    this.address = address;
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
   * @return The value of the {@code address} attribute
   */
  @Override
  public Address address() {
    return address;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddress#id() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddress withId(Long value) {
    Long newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return new PersonWithAddress(this, newValue, this.name, this.address);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddress#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddress withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new PersonWithAddress(this, this.id, newValue, this.address);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractPersonWithAddress#address() address} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for address
   * @return A modified copy of the {@code this} object
   */
  public final PersonWithAddress withAddress(Address value) {
    if (this.address == value) return this;
    Address newValue = Objects.requireNonNull(value, "address");
    return new PersonWithAddress(this, this.id, this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code PersonWithAddress} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof PersonWithAddress
        && equalTo((PersonWithAddress) another);
  }

  private boolean equalTo(PersonWithAddress another) {
    return id.equals(another.id)
        && name.equals(another.name)
        && address.equals(another.address);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code name}, {@code address}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + address.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code PersonWithAddress} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("PersonWithAddress")
        .omitNullValues()
        .add("id", id)
        .add("name", name)
        .add("address", address)
        .toString();
  }

  /**
   * Construct a new immutable {@code PersonWithAddress} instance.
   * @param id The value for the {@code id} attribute
   * @param name The value for the {@code name} attribute
   * @param address The value for the {@code address} attribute
   * @return An immutable PersonWithAddress instance
   */
  public static PersonWithAddress of(Long id, String name, Address address) {
    return new PersonWithAddress(id, name, address);
  }

  /**
   * Creates an immutable copy of a {@link AbstractPersonWithAddress} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PersonWithAddress instance
   */
  public static PersonWithAddress copyOf(AbstractPersonWithAddress instance) {
    if (instance instanceof PersonWithAddress) {
      return (PersonWithAddress) instance;
    }
    return PersonWithAddress.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link PersonWithAddress PersonWithAddress}.
   * <pre>
   * PersonWithAddress.builder()
   *    .id(Long) // required {@link AbstractPersonWithAddress#id() id}
   *    .name(String) // required {@link AbstractPersonWithAddress#name() name}
   *    .address(com.ajjpj.acollections.AList&amp;lt;de.Address&amp;gt;) // required {@link AbstractPersonWithAddress#address() address}
   *    .build();
   * </pre>
   * @return A new PersonWithAddress builder
   */
  public static PersonWithAddress.Builder builder() {
    return new PersonWithAddress.Builder();
  }

  /**
   * Builds instances of type {@link PersonWithAddress PersonWithAddress}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_ADDRESS = 0x4L;
    private long initBits = 0x7L;

    private @Nullable
    Long id;
    private @Nullable
    String name;
    private @Nullable
    Address address;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractPersonWithAddress} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder from(AbstractPersonWithAddress instance) {
      Objects.requireNonNull(instance, "instance");
      id(instance.id());
      name(instance.name());
      address(instance.address());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractPersonWithAddress#id() id} attribute.
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
     * Initializes the value for the {@link AbstractPersonWithAddress#name() name} attribute.
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
     * Initializes the value for the {@link AbstractPersonWithAddress#address() address} attribute.
     * @param address The value for address
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder address(Address address) {
      this.address = Objects.requireNonNull(address, "address");
      initBits &= ~INIT_BIT_ADDRESS;
      return this;
    }

    /**
     * Builds a new {@link PersonWithAddress PersonWithAddresses}.
     * @return An immutable instance of PersonWithAddresses
     * @throws IllegalStateException if any required attributes are missing
     */
    public PersonWithAddress build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new PersonWithAddress(null, id, name, address);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_ADDRESS) != 0) attributes.add("address");
      return "Cannot build PersonWithAddresses, some of required attributes are not set " + attributes;
    }
  }
}
