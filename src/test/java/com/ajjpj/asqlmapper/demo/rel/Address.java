package com.ajjpj.asqlmapper.demo.rel;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Immutable implementation of {@link AbstractAddress}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code Address.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code Address.of()}.
 */
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
@CheckReturnValue
public final class Address implements AbstractAddress {
  private final String street;
  private final String city;

  private Address(String street, String city) {
    this.street = Objects.requireNonNull(street, "street");
    this.city = Objects.requireNonNull(city, "city");
  }

  private Address(Address original, String street, String city) {
    this.street = street;
    this.city = city;
  }

  /**
   * @return The value of the {@code street} attribute
   */
  @Override
  public String street() {
    return street;
  }

  /**
   * @return The value of the {@code city} attribute
   */
  @Override
  public String city() {
    return city;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractAddress#street() street} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for street
   * @return A modified copy of the {@code this} object
   */
  public final Address withStreet(String value) {
    String newValue = Objects.requireNonNull(value, "street");
    if (this.street.equals(newValue)) return this;
    return new Address(this, newValue, this.city);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractAddress#city() city} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for city
   * @return A modified copy of the {@code this} object
   */
  public final Address withCity(String value) {
    String newValue = Objects.requireNonNull(value, "city");
    if (this.city.equals(newValue)) return this;
    return new Address(this, this.street, newValue);
  }

  /**
   * This instance is equal to all instances of {@code Address} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof Address
        && equalTo((Address) another);
  }

  private boolean equalTo(Address another) {
    return street.equals(another.street)
        && city.equals(another.city);
  }

  /**
   * Computes a hash code from attributes: {@code street}, {@code city}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + street.hashCode();
    h += (h << 5) + city.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Address} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Address")
        .omitNullValues()
        .add("street", street)
        .add("city", city)
        .toString();
  }

  /**
   * Construct a new immutable {@code Address} instance.
   * @param street The value for the {@code street} attribute
   * @param city The value for the {@code city} attribute
   * @return An immutable Address instance
   */
  public static Address of(String street, String city) {
    return new Address(street, city);
  }

  /**
   * Creates an immutable copy of a {@link AbstractAddress} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Address instance
   */
  public static Address copyOf(AbstractAddress instance) {
    if (instance instanceof Address) {
      return (Address) instance;
    }
    return Address.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link Address Address}.
   * <pre>
   * Address.builder()
   *    .street(String) // required {@link AbstractAddress#street() street}
   *    .city(String) // required {@link AbstractAddress#city() city}
   *    .build();
   * </pre>
   * @return A new Address builder
   */
  public static Address.Builder builder() {
    return new Address.Builder();
  }

  /**
   * Builds instances of type {@link Address Address}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_STREET = 0x1L;
    private static final long INIT_BIT_CITY = 0x2L;
    private long initBits = 0x3L;

    private @Nullable
    String street;
    private @Nullable
    String city;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractAddress} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder from(AbstractAddress instance) {
      Objects.requireNonNull(instance, "instance");
      street(instance.street());
      city(instance.city());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractAddress#street() street} attribute.
     * @param street The value for street
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder street(String street) {
      this.street = Objects.requireNonNull(street, "street");
      initBits &= ~INIT_BIT_STREET;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractAddress#city() city} attribute.
     * @param city The value for city
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder city(String city) {
      this.city = Objects.requireNonNull(city, "city");
      initBits &= ~INIT_BIT_CITY;
      return this;
    }

    /**
     * Builds a new {@link Address Address}.
     * @return An immutable instance of Address
     * @throws IllegalStateException if any required attributes are missing
     */
    public Address build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new Address(null, street, city);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_STREET) != 0) attributes.add("street");
      if ((initBits & INIT_BIT_CITY) != 0) attributes.add("city");
      return "Cannot build Address, some of required attributes are not set " + attributes;
    }
  }
}
