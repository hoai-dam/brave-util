package waterplan;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NoSaltWater implements WaterCubic {

    final Boolean salty = false;
    final Boolean dirt = false;
    Boolean contaminated;
    Boolean heavyMetal;
    String characteristic;


    public NoSaltWater(WaterCubic waterCubic) {
        this.contaminated = waterCubic.getContaminated();
        this.heavyMetal = waterCubic.getHeavyMetal();
        this.characteristic = waterCubic.getCharacteristic();
    }

}
