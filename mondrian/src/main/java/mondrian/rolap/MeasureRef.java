package mondrian.rolap;

/**
 * Typed reference to a physical measure for source-agnostic resolution.
 *
 * @param uniqueName  measure unique name, e.g. {@code [Measures].[Sales]}
 * @param cubeName    base cube name owning this measure
 * @param bitPosition bit position in the {@link RolapStar} column index,
 *                    or {@code -1} if unknown (resolve by name)
 */
public record MeasureRef(
    String uniqueName,
    String cubeName,
    int bitPosition) {}
