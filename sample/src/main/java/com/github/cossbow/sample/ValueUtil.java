package com.github.cossbow.sample;


import lombok.NonNull;

import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoField.*;


final
public class ValueUtil {
    private ValueUtil() {
    }

    public static boolean isUpperCase(char c) {
        return c >= 'A' && c <= 'Z';
    }

    public static String toQueryString(Map<String, ?> data) {
        var sb = new StringBuilder();
        boolean first = true;
        for (var pair : data.entrySet()) {
            var key = pair.getKey();
            var val = pair.getValue();
            if (null == key || null == val) continue;

            if (first) {
                first = false;
            } else {
                sb.append('&');
            }

            sb.append(key);
            sb.append("=");
            sb.append(URLEncoder.encode(val.toString(), StandardCharsets.UTF_8));
        }

        return sb.toString();
    }

    //
    // 空值检查
    //

    public static boolean isEmpty(String s) {
        return null == s || s.isEmpty();
    }

    public static boolean isEmpty(Collection<?> c) {
        return null == c || c.isEmpty();
    }

    public static boolean isNotEmpty(Collection<?> c) {
        return c != null && !c.isEmpty();
    }

    public static boolean isNotEmpty(Map<?, ?> c) {
        return c != null && !c.isEmpty();
    }

    public static boolean isNotEmpty(String c) {
        return c != null && !c.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> c) {
        return null == c || c.isEmpty();
    }

    public static boolean isEmpty(Object val) {
        return (val == null || "".equals(val));
    }

    public static boolean isEmpty(Object[] arr) {
        return null == arr || arr.length == 0;
    }

    public static boolean isEmpty(long[] arr) {
        return null == arr || arr.length == 0;
    }

    public static boolean isNotEmpty(long[] arr) {
        return arr != null && arr.length > 0;
    }

    public static <T> boolean isNotEmpty(T[] arr) {
        return arr != null && arr.length > 0;
    }

    public static int len(long[] a) {
        return null == a ? 0 : a.length;
    }

    public static int len(Object[] a) {
        return null == a ? 0 : a.length;
    }

    public static int len(Collection<?> a) {
        return null == a ? 0 : a.size();
    }

    public static int len(Map<?, ?> a) {
        return null == a ? 0 : a.size();
    }

    public static String emptyIfNull(String s) {
        return null == s ? "" : s;
    }

    public static <T> List<T> emptyIfNull(List<T> s) {
        return null == s ? List.of() : s;
    }

    public static <T> Set<T> emptyIfNull(Set<T> s) {
        return null == s ? Set.of() : s;
    }

    public static <K, V> Map<K, V> emptyIfNull(Map<K, V> s) {
        return null == s ? Map.of() : s;
    }


    //
    // arrays
    //

    public static final IntFunction<String[]> ARRAY_CONSTRUCTOR_STRING = String[]::new;
    public static final IntFunction<Integer[]> ARRAY_CONSTRUCTOR_INTEGER = Integer[]::new;
    public static final IntFunction<Long[]> ARRAY_CONSTRUCTOR_LONG = Long[]::new;

    @SafeVarargs
    public static <T> T[] mergeArrays(IntFunction<T[]> constructor, T[]... arrays) {
        if (isEmpty(arrays)) return constructor.apply(0);
        int total = 0;
        for (var array : arrays) {
            if (null != array && array.length > 0) {
                total += array.length;
            }
        }
        var re = constructor.apply(total);
        int offset = 0;
        for (var array : arrays) {
            if (null != array && array.length > 0) {
                System.arraycopy(array, 0, re, offset, array.length);
                offset += array.length;
            }
        }
        return re;
    }

    public static <T> void clear(T[] array) {
        if (null == array) return;

        for (int i = 0; i < array.length; i++) {
            array[i] = null;
        }
    }

    //
    // String
    //

    public static String findNumber(String s) {
        if (isEmpty(s)) return null;

        boolean hasNumber = false;
        var sb = new StringBuilder(s.length());
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final var c = s.charAt(i);
            if ('0' <= c && c <= '9') {
                sb.append(c);
                hasNumber = true;
            } else {
                if (hasNumber) {
                    break;
                }
            }
        }

        return sb.length() > 0 ? sb.toString() : null;
    }

    public static List<String> findNumbers(String s) {
        Objects.requireNonNull(s);

        var li = new ArrayList<String>();

        var sb = new StringBuilder(s.length());
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final var c = s.charAt(i);
            if ('0' <= c && c <= '9') {
                sb.append(c);
            } else {
                if (sb.length() > 0) {
                    li.add(sb.toString());
                    sb.setLength(0);
                }
            }
        }

        return li;
    }

    //
    // 货币值
    //

    /**
     * 货币[元]转换[分]
     */
    public static int toCent(float m) {
        return Math.round(m * 100);
    }

    public static int toCent(@NonNull BigDecimal m) {
        return (int) (m.intValue() * 100);
    }

    /**
     * 货币[分]转换[元]
     */
    public static float fromCent(int c) {
        return (float) c / 100;
    }

    /**
     * 货币[分]转换[元]并格式化
     */
    public static String fromCentAsStr(int c) {
        if (0 == c) {
            return "0";
        }
        var sb = new StringBuilder();
        int a = c / 100;
        int b = c - a * 100;
        sb.append(a).append('.');
        if (b > 0) {
            if (b < 10) {
                sb.append('0');
            }
            sb.append(b);
        } else {
            sb.append("00");
        }
        return sb.toString();
    }

    public static BigDecimal fromCentAsDec(int c) {
        return new BigDecimal(fromCent(c));
    }


    //
    //
    //


    public static int max(int... arr) {
        var m = Integer.MIN_VALUE;
        for (var an : arr) {
            if (m < an) m = an;
        }
        return m;
    }

    public static long max(long... arr) {
        var m = Long.MIN_VALUE;
        for (var an : arr) {
            if (m < an) m = an;
        }
        return m;
    }


    public static int min(int... arr) {
        var m = Integer.MAX_VALUE;
        for (var an : arr) {
            if (m > an) m = an;
        }
        return m;
    }

    public static long min(long... arr) {
        var m = Long.MAX_VALUE;
        for (var an : arr) {
            if (m > an) m = an;
        }
        return m;
    }


    //
    //
    //

    public static final short BOOL_TRUE = 1;
    public static final short BOOL_FALSE = 0;

    public static short boolValue(boolean v) {
        return v ? BOOL_TRUE : BOOL_FALSE;
    }

    public static boolean boolValue(short v) {
        return BOOL_TRUE == v;
    }


    //
    //
    //

    public static <T> void slice(List<T> list, int subSize, Consumer<List<T>> consumer) {
        if (list.size() <= subSize) {
            consumer.accept(list);
            return;
        }

        int totalSize = list.size();
        for (int i = 0; i < totalSize; i += subSize) {
            int end = Math.min(totalSize, i + subSize);
            var subList = list.subList(i, end);
            consumer.accept(subList);
        }
    }

    public static <T, R> Stream<R> slice(List<T> list, int subSize, Function<List<T>, R> applier) {
        if (list.isEmpty()) return Stream.empty();
        if (list.size() <= subSize) return Stream.of(applier.apply(list));

        var rs = Stream.<R>builder();
        int totalSize = list.size();
        for (int i = 0; i < totalSize; i += subSize) {
            int end = Math.min(totalSize, i + subSize);
            var subList = list.subList(i, end);
            R r = applier.apply(subList);
            rs.add(r);
        }

        return rs.build();
    }

    public static void slice(long[] src, int size, Consumer<long[]> consumer) {
        if (src.length == 0) return;

        for (int i = 0; i < src.length; i += size) {
            int newLength = Math.min(size, src.length - i);
            long[] ids = new long[newLength];
            System.arraycopy(src, i, ids, 0, newLength);
            consumer.accept(ids);
        }
    }

    public static <R> Stream<R> slice(long[] src, int size, Function<long[], R> applier) {
        var rs = Stream.<R>builder();
        if (src.length == 0) return rs.build();

        for (int i = 0; i < src.length; i += size) {
            int newLength = Math.min(size, src.length - i);
            long[] ids = new long[newLength];
            System.arraycopy(src, i, ids, 0, newLength);
            R r = applier.apply(ids);
            rs.add(r);
        }

        return rs.build();
    }

    public static List<long[]> slice(long[] src, int size) {
        if (isEmpty(src)) return List.of();

        var result = new ArrayList<long[]>((int) Math.ceil(src.length / (float) size));
        for (int i = 0; i < src.length; i += size) {
            int newLength = Math.min(size, src.length - i);
            var ids = new long[newLength];
            System.arraycopy(src, i, ids, 0, newLength);
            result.add(ids);
        }

        return result;
    }

    //

    public static <S> int sum(Collection<S> c, ToIntFunction<S> mapper) {
        int n = 0;
        for (var t : c) n += mapper.applyAsInt(t);
        return n;
    }

    public static <S> long sum(Collection<S> c, ToLongFunction<S> mapper) {
        int n = 0;
        for (var t : c) n += mapper.applyAsLong(t);
        return n;
    }

    //

    public static <S> List<S> mapToList(Collection<S> c) {
        return mapTo(c, ArrayList::new);
    }

    public static <S, T> List<T> mapToList(Collection<S> c, Function<S, T> mapper) {
        return mapTo(c, ArrayList::new, mapper);
    }

    public static <S> Set<S> mapToSet(Collection<S> c) {
        return mapTo(c, HashSet::new);
    }

    public static <S, T> Set<T> mapToSet(Collection<S> c, Function<S, T> mapper) {
        return mapTo(c, HashSet::new, mapper);
    }

    public static <S, C extends Collection<S>> C mapTo(Collection<S> c, IntFunction<C> creator) {
        return mapTo(c, creator, Function.identity());
    }

    public static <S, T, C extends Collection<T>> C mapTo(Collection<S> c, IntFunction<C> creator, Function<S, T> mapper) {
        var l = creator.apply(c.size());
        for (var t : c) l.add(mapper.apply(t));
        return l;
    }


    //

    public static <T extends Comparable<T>> T min(T a, T b) {
        return a.compareTo(b) < 0 ? a : b;
    }

    public static <T extends Comparable<T>> T max(T a, T b) {
        return a.compareTo(b) > 0 ? a : b;
    }


    //

    public static <T extends Comparable<T>> Set<T>
    intersection(final Set<T> set1, final Set<T> set2) {
        Objects.requireNonNull(set1, "set1");
        Objects.requireNonNull(set2, "set2");

        var s = new HashSet<T>(Integer.min(set1.size(), set2.size()));
        for (var t : set1) {
            if (set2.contains(t)) s.add(t);
        }

        return s;
    }


    //

    public static <T> Supplier<T> nullSupplier() {
        return () -> null;
    }

    public static <T, R> Function<T, R> nullFunction() {
        return (r) -> null;
    }


    public static <S> LongFunction<S> finderWithId(Collection<S> c, ToLongFunction<S> idMapper) {
        return id -> {
            for (var s : c) if (idMapper.applyAsLong(s) == id) return s;
            return null;
        };
    }

    public static <S> LongFunction<Optional<S>> finderById(Collection<S> c, ToLongFunction<S> idMapper) {
        return id -> {
            for (var s : c) if (idMapper.applyAsLong(s) == id) return Optional.ofNullable(s);
            return Optional.empty();
        };
    }

    public static <S, T> Function<T, S> finderWithKey(Collection<S> c, Function<S, T> idMapper) {
        var map = c.stream().collect(Collectors.toMap(idMapper, Function.identity()));
        return map::get;
    }

    public static <S, T> Function<T, Optional<S>> finderByKey(Collection<S> c, Function<S, T> idMapper) {
        var map = c.stream().collect(Collectors.toMap(idMapper, Function.identity()));
        return id -> null == id ? Optional.empty() : Optional.ofNullable(map.get(id));
    }


    //
    // DateTime For Local [China time]
    //

    public static final ZoneOffset ZONE_CHINA = ZoneOffset.ofHours(8);
    public static final DateTimeFormatter FORMATTER_LOCAL_DATETIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();

    private static final Map<LocalDate, String> DATE_LD_STR_MAP = new ConcurrentHashMap<>();

    /**
     * @param date
     * @return yyyy-mm-dd
     */
    public static String date(@NonNull Instant date) {
        return date(LocalDate.ofInstant(date, ZONE_CHINA));
    }

    /**
     * @return not thread safe cache
     */
    public static Function<Instant, String> dateFormatCache() {
        var cache = new HashMap<Instant, String>(32);
        return date -> {
            Objects.requireNonNull(date);
            return cache.computeIfAbsent(date, d -> {
                return date(LocalDate.ofInstant(d, ZONE_CHINA));
            });
        };
    }

    public static String date(@NonNull LocalDate date) {
        return DATE_LD_STR_MAP.computeIfAbsent(date, d -> d.format(DateTimeFormatter.ISO_LOCAL_DATE));
    }

    /**
     * @param instant {@link Instant}
     * @return yyyy-mm-dd {@link LocalDate}
     */
    public static LocalDate toDate(@NonNull Instant instant) {
        return LocalDate.ofInstant(instant, ZONE_CHINA);
    }

    /**
     * @param epochSeconds long
     * @return {@link LocalDate}
     */
    public static LocalDate toDate(long epochSeconds) {
        long localSecond = epochSeconds + ZONE_CHINA.getTotalSeconds();
        long localEpochDay = Math.floorDiv(localSecond, 3600 * 24);
        return LocalDate.ofEpochDay(localEpochDay);
    }

    public static String datetime(@NonNull Instant instant) {
        return LocalDateTime.ofInstant(instant, ZONE_CHINA).format(FORMATTER_LOCAL_DATETIME);
    }

    public static String date(@NonNull LocalDateTime dateTime) {
        return date(dateTime.toLocalDate());
    }

    public static String datetime(@NonNull LocalDateTime dateTime) {
        return dateTime.format(FORMATTER_LOCAL_DATETIME);
    }

    public static String datetime(@NonNull OffsetDateTime dateTime) {
        return dateTime.format(FORMATTER_LOCAL_DATETIME);
    }

    public static Instant fromDate(@NonNull LocalDate date) {
        return Instant.ofEpochSecond(date.toEpochSecond(LocalTime.MIN, ZONE_CHINA));
    }

    /**
     * @return not thread safe cache
     */
    public static Function<LocalDate, Instant> fromDateCache() {
        var cache = new HashMap<LocalDate, Instant>(32);
        return (date) -> {
            Objects.requireNonNull(date);
            return cache.computeIfAbsent(date, d -> {
                return Instant.ofEpochSecond(date.toEpochSecond(LocalTime.MIN, ZONE_CHINA));
            });
        };
    }

    public static Instant fromDatetime(@NonNull LocalDateTime dateTime) {
        return dateTime.toInstant(ZONE_CHINA);
    }

    /**
     * @return 当天零点的时间戳
     */
    public static Instant todayZero() {
        return fromDate(today());
    }

    public static LocalDate today() {
        return LocalDate.ofInstant(Instant.now(), ZONE_CHINA);
    }

    public static long currentSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    public static Instant parseDatetime(String text) {
        return LocalDateTime.parse(text, FORMATTER_LOCAL_DATETIME).toInstant(ZONE_CHINA);
    }

    public static LocalDateTime datetime(String text) {
        return LocalDateTime.parse(text, FORMATTER_LOCAL_DATETIME);
    }

    public static Function<String, Instant> parseDateCache() {
        var cache = new HashMap<String, Instant>(32);
        return (str) -> {
            Objects.requireNonNull(str);
            return cache.computeIfAbsent(str, s -> {
                return fromDate(LocalDate.parse(s));
            });
        };
    }

    public static List<Instant> makeDateList(@NonNull Instant start, @NonNull Instant end) {
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("start must before end");
        }
        Predicate<Instant> hasNext = i -> i.isBefore(end);
        return Stream.iterate(start, hasNext, i -> i.plus(1, ChronoUnit.DAYS))
                .collect(Collectors.toList());
    }


    public static List<Instant> makeDateList(@NonNull LocalDate start, int days) {
        return makeDateList(fromDate(start), days);
    }

    public static List<Instant> makeDateList(@NonNull Instant first, int days) {
        if (days < 1) throw new IllegalArgumentException("days must be positive integer");

        var dates = new ArrayList<Instant>(days);
        dates.add(first);
        for (int i = 1; i < days; i++) {
            dates.add(first.plus(i, ChronoUnit.DAYS));
        }

        return dates;
    }

    public static List<Instant> makeDateList(@NonNull LocalDate start, @NonNull LocalDate end) {
        return makeDateList(fromDate(start), fromDate(end));
    }


    /**
     * 计算从start到end的天数
     */
    public static int days(@NonNull Temporal start, @NonNull Temporal end) {
        return (int) ChronoUnit.DAYS.between(start, end);
    }

    public static List<?> split(Instant start, Instant end, int interval, ChronoUnit unit) {
        return null;
    }

    //
    //
    //


    @SafeVarargs
    public static <T> List<T> asList(T... args) {
        var list = new ArrayList<T>(args.length);
        for (int i = 0; i < args.length; i++) {
            list.add(args[i]);
        }
        return list;
    }

    public static List<Long> asList(long[] args) {
        return asList(args, Long::valueOf);
    }

    public static <T> List<T> asList(long[] args, LongFunction<T> trans) {
        return asList(args, ArrayList::new, trans);
    }

    public static <T> List<T> asList(long[] args, IntFunction<List<T>> supplier, LongFunction<T> trans) {
        var result = supplier.apply(args.length);
        for (var item : args) result.add(trans.apply(item));
        return result;
    }

    public static <T> List<T> asList(Collection<T> c) {
        return new ArrayList<>(c);
    }

    public static long[] toArray(Collection<Long> collection) {
        if (isEmpty(collection)) return new long[0];

        var size = collection.size();
        var result = new long[size];
        var i = 0;
        for (var item : collection) result[i++] = item;
        return result;
    }


    public static <T, K, V, M extends Map<K, V>> M toMap(Collection<T> collection,
                                                         Function<T, K> keyMapper,
                                                         Function<T, V> valueMapper,
                                                         IntFunction<M> supplier) {
        var map = supplier.apply(collection.size());
        for (var item : collection) {
            var key = keyMapper.apply(item);
            var value = valueMapper.apply(item);
            var ov = map.putIfAbsent(key, value);
            if (ov != null) {
                throw new IllegalStateException(String.format(
                        "Duplicate key %s (attempted merging values %s and %s)",
                        key, ov, value));
            }
        }
        return map;
    }

    public static <K, V, M extends Map<K, V>> M toMap(Collection<V> collection,
                                                      Function<V, K> keyMapper,
                                                      IntFunction<M> supplier) {
        return toMap(collection, keyMapper, Function.identity(), supplier);
    }

    public static <T, K, V> Map<K, V> toHashMap(Collection<T> collection,
                                                Function<T, K> keyMapper,
                                                Function<T, V> valueMapper) {
        return toMap(collection, keyMapper, valueMapper, HashMap::new);
    }

    public static <K, V> Map<K, V> toHashMap(Collection<V> collection,
                                             Function<V, K> keyMapper) {
        return toMap(collection, keyMapper, HashMap::new);
    }


    public static <K, V> Map<K, V> asMap(K k, V v) {
        var m = new HashMap<K, V>(2);
        m.put(k, v);
        return m;
    }

    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2) {
        var m = new HashMap<K, V>(2);
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2, K k3, V v3) {
        var m = new HashMap<K, V>(4);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return m;
    }

    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        var m = new HashMap<K, V>(4);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        m.put(k4, v4);
        return m;
    }

    //
    //
    //

}
