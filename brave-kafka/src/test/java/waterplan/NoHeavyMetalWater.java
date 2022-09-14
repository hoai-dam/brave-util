package waterplan;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NoHeavyMetalWater implements WaterCubic {

    final Boolean salty = false;
    final Boolean dirt = false;
    Boolean contaminated;
    final Boolean heavyMetal = false;
    String characteristic;


    public NoHeavyMetalWater(WaterCubic waterCubic) {
        this.contaminated = waterCubic.getContaminated();
        this.characteristic = waterCubic.getCharacteristic();
    }

}
